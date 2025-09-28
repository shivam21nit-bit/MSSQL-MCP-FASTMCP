"""
DOTA — Data Origin & Traceability Assistant (FastMCP v2.x)
----------------------------------------------------------
SQL Server metadata + dynamic connection switching + lineage (population logic)
with synonym resolution and client-side-friendly topology (no Graphviz).

HTTP (default): agent connects at /mcp/
STDIO: fallback if MCP_HTTP=0

Requirements:
  pip install fastmcp python-dotenv pyodbc
  # optional for tunneling:
  # pip install cloudflared
"""

import os
import re
import logging
import threading
from typing import Dict, Optional, Tuple, List, Any
from contextlib import contextmanager
from functools import lru_cache
from datetime import datetime, timedelta

import pyodbc
from dotenv import load_dotenv, set_key
from fastmcp import FastMCP

# -------------------------------------------------------------------
# Env & Logging
# -------------------------------------------------------------------
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("dota-mcp")

# ODBC connection pooling (explicit)
pyodbc.pooling = True

# ---- Config & limits ----
DEFAULT_LINEAGE_MAX_DEPTH = int(os.getenv("LINEAGE_MAX_DEPTH", "5"))   # variable default
MAX_ALLOWED_LINEAGE_DEPTH = 10                                        # hard cap
MAX_PROC_SCAN = int(os.getenv("MAX_PROC_SCAN", "3000"))               # cap fallback scans
USE_THREADLOCAL_CONN = os.getenv("DB_THREADLOCAL", "0") == "1"        # optional TL reuse

# Provenance: expose DB name only (no server) — default ON
EXPOSE_DATABASE_ONLY = os.getenv("DOTA_EXPOSE_DATABASE", "1") == "1"

# Default include behavior for definitions in lineage/population ("none"|"excerpt"|"full")
DEFAULT_INCLUDE_DEFS = os.getenv("DOTA_INCLUDE_DEFS", "excerpt").lower()
if DEFAULT_INCLUDE_DEFS not in ("none", "excerpt", "full"):
    DEFAULT_INCLUDE_DEFS = "excerpt"

DB_CONFIG: Dict[str, object] = {
    "server": os.getenv("DB_SERVER"),
    "database": os.getenv("DB_NAME"),
    "username": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASS"),
    "driver": os.getenv("DB_DRIVER", "{ODBC Driver 17 for SQL Server}"),
    "timeout": int(os.getenv("DB_TIMEOUT", "30")),
    "login_timeout": int(os.getenv("DB_LOGIN_TIMEOUT", "15")),
}

# Locks
_config_lock = threading.RLock()   # DB_CONFIG changes
_schema_lock = threading.RLock()   # schema & proc/deps caches

# Optional: thread-local connections
_tls = threading.local()

def _build_conn_str(cfg: Dict[str, object]) -> str:
    return (
        f"DRIVER={cfg['driver']};"
        f"SERVER={cfg['server']};"
        f"DATABASE={cfg['database']};"
        f"UID={cfg['username']};"
        f"PWD={cfg['password']};"
        f"Timeout={cfg['timeout']};"
        f"LoginTimeout={cfg['login_timeout']}"
    )

def _get_tl_conn():
    conn = getattr(_tls, "conn", None)
    if conn is None:
        conn = pyodbc.connect(_build_conn_str(DB_CONFIG), autocommit=True)
        _tls.conn = conn
    return conn

def get_db_connection():
    with _config_lock:
        return _get_tl_conn() if USE_THREADLOCAL_CONN else pyodbc.connect(_build_conn_str(DB_CONFIG), autocommit=True)

@contextmanager
def db_cursor():
    """General cursor (default isolation)."""
    conn = get_db_connection()
    try:
        yield conn.cursor()
    finally:
        if not USE_THREADLOCAL_CONN:
            conn.close()

@contextmanager
def metadata_cursor():
    """
    Metadata cursor that avoids blocking via READ UNCOMMITTED.
    Only for catalog queries; DO NOT use for transactional data reads.
    """
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;")
        yield cur
    finally:
        if not USE_THREADLOCAL_CONN:
            conn.close()

def _test_connection(cfg: Dict[str, object]) -> Tuple[bool, Optional[str]]:
    try:
        conn = pyodbc.connect(_build_conn_str(cfg), autocommit=True)
        conn.close()
        return True, None
    except Exception as e:
        return False, str(e)

def set_db_config(
    server: str,
    database: str,
    username: str,
    password: str,
    driver: Optional[str] = None,
    timeout: Optional[int] = None,
    login_timeout: Optional[int] = None,
) -> Dict[str, object]:
    proposed = {
        "server": server,
        "database": database,
        "username": username,
        "password": password,
        "driver": driver or DB_CONFIG.get("driver", "{ODBC Driver 17 for SQL Server}"),
        "timeout": int(timeout if timeout is not None else DB_CONFIG.get("timeout", 30)),
        "login_timeout": int(login_timeout if login_timeout is not None else DB_CONFIG.get("login_timeout", 15)),
    }

    ok, err = _test_connection(proposed)
    if not ok:
        raise ValueError(f"Connection failed: {err}")

    with _config_lock:
        DB_CONFIG.update(proposed)

    # Close any thread-local connection so the next call re-opens with new config
    if USE_THREADLOCAL_CONN:
        old = getattr(_tls, "conn", None)
        if old:
            try: old.close()
            except: pass
            _tls.conn = None

    # Invalidate caches on switch
    with _schema_lock:
        for k in list(db_schema_cache.keys()):
            db_schema_cache[k] = {} if isinstance(db_schema_cache[k], dict) else {}
    _lineage_core.cache_clear()

    counts = load_schema_cache()
    return {"success": True, "connected_to": {"server": server, "database": database}, "schema_counts": counts}

# -------------------------------------------------------------------
# In-memory Caches (copy-on-write updates)
# -------------------------------------------------------------------
db_schema_cache: Dict[str, Any] = {
    "tables": {},           # {lower_table_name: TableName}
    "columns": {},          # {lower_table_name: {lower_col: ColName}}
    "columns_index": {},    # {lower_column_name: [schema.table, ...]}
    "objects": {},          # {lower_key -> QualifiedName} (stores qualified & unqualified keys)
    "jobs": {},             # {lower_job_name: JobName}
    "procedures": {},       # {object_id: {object_id, schema, name, definition}}
    "rev_deps": {},         # {(schema_lower, name_lower): set(proc_object_id, ...)}
    "synonyms": {},         # {(syn_schema_lower, syn_name_lower): {...}}
    "synonyms_by_base": {}  # {(base_schema_lower, base_name_lower): [(syn_schema, syn_name), ...]}
}

# -------------------------------------------------------------------
# Cache Loader (copy-on-write)
# -------------------------------------------------------------------
def load_schema_cache() -> Dict[str, int]:
    new_tables: Dict[str, str] = {}
    new_columns: Dict[str, Dict[str, str]] = {}
    new_col_index: Dict[str, List[str]] = {}
    new_objects: Dict[str, str] = {}
    new_jobs: Dict[str, str] = {}
    new_procs: Dict[int, Dict[str, Any]] = {}
    new_revdeps: Dict[Tuple[str, str], set] = {}
    new_synonyms: Dict[Tuple[str, str], Dict[str, Any]] = {}
    new_syn_by_base: Dict[Tuple[str, str], List[Tuple[str, str]]] = {}

    with metadata_cursor() as cursor:
        # Tables
        cursor.execute("SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'")
        tbl_rows = cursor.fetchall()
        for r in tbl_rows:
            new_tables[r.TABLE_NAME.lower()] = r.TABLE_NAME

        # Columns + fully-qualified column index
        for t in tbl_rows:
            cursor.execute(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=?",
                t.TABLE_SCHEMA, t.TABLE_NAME,
            )
            cols = [c.COLUMN_NAME for c in cursor.fetchall()]
            new_columns[t.TABLE_NAME.lower()] = {c.lower(): c for c in cols}
            fq = f"{t.TABLE_SCHEMA}.{t.TABLE_NAME}"
            for c in cols:
                new_col_index.setdefault(c.lower(), []).append(fq)

        # Objects (routines + views) — store BOTH qualified and unqualified keys
        cursor.execute("""
            SELECT ROUTINE_SCHEMA AS obj_schema, ROUTINE_NAME AS obj_name
            FROM INFORMATION_SCHEMA.ROUTINES
            UNION ALL
            SELECT TABLE_SCHEMA  AS obj_schema, TABLE_NAME  AS obj_name
            FROM INFORMATION_SCHEMA.VIEWS
        """)
        for r in cursor.fetchall():
            qname = f"{r.obj_schema}.{r.obj_name}"
            new_objects[r.obj_name.lower()] = qname          # unqualified key -> qualified
            new_objects[qname.lower()]      = qname          # qualified key   -> qualified

        # Jobs (best-effort)
        try:
            cursor.execute("SELECT name FROM msdb.dbo.sysjobs")
            for r in cursor.fetchall():
                new_jobs[r.name.lower()] = r.name
        except Exception:
            pass

        # Procedures + definitions
        try:
            cursor.execute("""
                SELECT p.object_id,
                       OBJECT_SCHEMA_NAME(p.object_id) AS proc_schema,
                       OBJECT_NAME(p.object_id) AS proc_name,
                       m.definition
                FROM sys.procedures p
                JOIN sys.sql_modules m ON m.object_id = p.object_id
            """)
            for r in cursor.fetchall():
                new_procs[r.object_id] = {
                    "object_id": r.object_id,
                    "schema": r.proc_schema,
                    "name": r.proc_name,
                    "definition": r.definition or "",
                }
        except Exception:
            new_procs = {}

        # Reverse dependency index
        try:
            cursor.execute("""
                SELECT d.referencing_id, d.referenced_id,
                       OBJECT_SCHEMA_NAME(d.referenced_id) AS ref_schema,
                       OBJECT_NAME(d.referenced_id) AS ref_name,
                       o.[type] AS ref_type,
                       o2.[type] AS referencing_type
                FROM sys.sql_expression_dependencies d
                LEFT JOIN sys.objects o   ON o.object_id  = d.referenced_id
                LEFT JOIN sys.objects o2  ON o2.object_id = d.referencing_id
            """)
            for r in cursor.fetchall():
                if r.referencing_type != 'P' or not r.ref_schema or not r.ref_name:
                    continue
                key = (r.ref_schema.lower(), r.ref_name.lower())
                s = new_revdeps.get(key)
                if s is None:
                    s = set()
                    new_revdeps[key] = s
                s.add(r.referencing_id)
        except Exception:
            new_revdeps = {}

        # Synonyms + reverse mapping
        try:
            cursor.execute("""
                SELECT s.name AS syn_name,
                       SCHEMA_NAME(s.schema_id) AS syn_schema,
                       PARSENAME(s.base_object_name, 1) AS base_object,
                       PARSENAME(s.base_object_name, 2) AS base_schema,
                       PARSENAME(s.base_object_name, 3) AS base_db,
                       PARSENAME(s.base_object_name, 4) AS base_server
                FROM sys.synonyms s
            """)
            for r in cursor.fetchall():
                syn_key = (r.syn_schema.lower(), r.syn_name.lower())
                new_synonyms[syn_key] = {
                    "syn_schema": r.syn_schema,
                    "syn_name": r.syn_name,
                    "base_schema": r.base_schema,
                    "base_name": r.base_object,
                    "base_db": r.base_db,
                    "base_server": r.base_server,
                }
                if r.base_schema and r.base_object:
                    bk = (r.base_schema.lower(), r.base_object.lower())
                    new_syn_by_base.setdefault(bk, []).append((r.syn_schema, r.syn_name))
        except Exception:
            new_synonyms = {}
            new_syn_by_base = {}

    # Swap under the lock
    with _schema_lock:
        db_schema_cache["tables"] = new_tables
        db_schema_cache["columns"] = new_columns
        db_schema_cache["columns_index"] = new_col_index
        db_schema_cache["objects"] = new_objects
        db_schema_cache["jobs"] = new_jobs
        db_schema_cache["procedures"] = new_procs
        db_schema_cache["rev_deps"] = new_revdeps
        db_schema_cache["synonyms"] = new_synonyms
        db_schema_cache["synonyms_by_base"] = new_syn_by_base

    return {
        "tables": len(new_tables),
        "objects": len(new_objects),
        "jobs": len(new_jobs),
        "procedures": len(new_procs),
        "synonyms": len(new_synonyms),
    }

# -------------------------------------------------------------------
# Validators & Utils
# -------------------------------------------------------------------
def validate_table_column(table: str, column: Optional[str] = None) -> Tuple[Tuple[str, str], Optional[str]]:
    schema, real_table = _get_table_schema_and_name(table)
    real_column = None
    if column:
        with metadata_cursor() as cursor:
            cursor.execute("""
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND LOWER(COLUMN_NAME) = LOWER(?)
            """, schema, real_table, column)
            r = cursor.fetchone()
            if not r:
                raise ValueError(f"Column '{column}' not found in table '{schema}.{real_table}'.")
            real_column = r.COLUMN_NAME
    return (schema, real_table), real_column

def _get_table_schema_and_name(table: str) -> Tuple[str, str]:
    with metadata_cursor() as cursor:
        # direct match (bare)
        cursor.execute("""
            SELECT TABLE_SCHEMA, TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE='BASE TABLE' AND LOWER(TABLE_NAME) = LOWER(?)
        """, table)
        row = cursor.fetchone()
        if row:
            return row.TABLE_SCHEMA, row.TABLE_NAME

        # schema-qualified
        if "." in table:
            schema, tname = table.split(".", 1)
            cursor.execute("""
                SELECT TABLE_SCHEMA, TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE='BASE TABLE'
                  AND LOWER(TABLE_SCHEMA) = LOWER(?)
                  AND LOWER(TABLE_NAME) = LOWER(?)
            """, schema, tname)
            row = cursor.fetchone()
            if row:
                return row.TABLE_SCHEMA, row.TABLE_NAME

        # synonym (bare)
        cursor.execute("""
            SELECT PARSENAME(s.base_object_name,2) AS base_schema,
                   PARSENAME(s.base_object_name,1) AS base_object
            FROM sys.synonyms s
            WHERE LOWER(s.name) = LOWER(?)
        """, table)
        row = cursor.fetchone()
        if row and row.base_schema and row.base_object:
            return row.base_schema, row.base_object

        # synonym (schema-qualified)
        if "." in table:
            schema, sname = table.split(".", 1)
            cursor.execute("""
                SELECT PARSENAME(s.base_object_name,2) AS base_schema,
                       PARSENAME(s.base_object_name,1) AS base_object
                FROM sys.synonyms s
                WHERE LOWER(SCHEMA_NAME(s.schema_id)) = LOWER(?) AND LOWER(s.name) = LOWER(?)
            """, schema, sname)
            row = cursor.fetchone()
            if row and row.base_schema and row.base_object:
                return row.base_schema, row.base_object

    raise ValueError(f"Table '{table}' not found (as base table or synonym).")

def _object_id(schema: str, name: str) -> Optional[int]:
    with metadata_cursor() as cursor:
        cursor.execute("SELECT OBJECT_ID(QUOTENAME(?) + '.' + QUOTENAME(?))", schema, name)
        r = cursor.fetchone()
        return r[0] if r and r[0] else None

# --- Regex Parsers for Assignments ---
_RE_UPDATE_SET = re.compile(
    r"""UPDATE\s+(?P<tgt>[\[\]A-Za-z0-9_\.]+)\s+SET\s+(?P<sets>.+?)\s+(?:WHERE|OUTPUT|OPTION|;|$)""",
    re.IGNORECASE | re.DOTALL,
)
_RE_SET_PAIR = re.compile(
    r"""(?P<col>\[?[A-Za-z0-9_]+\]?)[ \t]*=[ \t]*(?P<expr>[^,]+)""",
    re.IGNORECASE,
)
_RE_INSERT_SELECT = re.compile(
    r"""INSERT\s+INTO\s+(?P<tgt>[\[\]A-Za-z0-9_\.]+)\s*\((?P<cols>.*?)\)\s*SELECT\s+(?P<select>.*?)\s+(?:FROM|VALUES|\(|WITH|OUTPUT|OPTION|;|$)""",
    re.IGNORECASE | re.DOTALL,
)
_RE_MERGE_UPDATE = re.compile(
    r"""MERGE\s+(?P<tgt>[\[\]A-Za-z0-9_\.]+)\s+AS\s+\w+.*?WHEN\s+MATCHED\s+THEN\s+UPDATE\s+SET\s+(?P<sets>.+?)(?:WHEN|OUTPUT|;|$)""",
    re.IGNORECASE | re.DOTALL,
)
_RE_MERGE_INSERT = re.compile(
    r"""MERGE\s+(?P<tgt>[\[\]A-Za-z0-9_\.]+).*?WHEN\s+NOT\s+MATCHED\s+THEN\s+INSERT\s*\((?P<cols>.*?)\)\s*VALUES\s*\((?P<vals>.*?)\)""",
    re.IGNORECASE | re.DOTALL,
)
# NEW: INSERT ... VALUES (non-MERGE)
_RE_INSERT_VALUES = re.compile(
    r"""INSERT\s+INTO\s+(?P<tgt>[\[\]A-Za-z0-9_\.]+)\s*
         \((?P<cols>.*?)\)\s*
         VALUES\s*\((?P<vals>.*?)\)""",
    re.IGNORECASE | re.DOTALL | re.VERBOSE,
)

def _normalize_brackets(s: str) -> str:
    return s.replace("[", "").replace("]", "").strip()

def _split_csv(expr: str) -> List[str]:
    parts, buf, depth = [], "", 0
    for ch in expr:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth = max(0, depth - 1)
        if ch == "," and depth == 0:
            parts.append(buf.strip()); buf = ""
        else:
            buf += ch
    if buf.strip():
        parts.append(buf.strip())
    return parts

def _split_set_list(sets: str) -> List[Tuple[str, str]]:
    parts = _split_csv(sets)
    out: List[Tuple[str, str]] = []
    for p in parts:
        depth = 0; eq_idx = -1
        for i,ch in enumerate(p):
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth = max(0, depth-1)
            elif ch == '=' and depth == 0:
                eq_idx = i; break
        if eq_idx > 0:
            col = _normalize_brackets(p[:eq_idx].strip())
            expr = p[eq_idx+1:].strip()
            out.append((col, expr))
    return out

def _extract_update_sets(defn: str, target_col: str) -> List[str]:
    exprs: List[str] = []
    for m in _RE_UPDATE_SET.finditer(defn):
        for col, expr in _split_set_list(m.group("sets")):
            if col.lower() == target_col.lower():
                exprs.append(expr)
    return exprs

def _extract_merge_update_sets(defn: str, target_col: str) -> List[str]:
    exprs: List[str] = []
    for m in _RE_MERGE_UPDATE.finditer(defn):
        for col, expr in _split_set_list(m.group("sets")):
            if col.lower() == target_col.lower():
                exprs.append(expr)
    return exprs

def _extract_insert_select(defn: str, target_col: str) -> List[str]:
    exprs: List[str] = []
    for m in _RE_INSERT_SELECT.finditer(defn):
        cols = [_normalize_brackets(c).lower() for c in _split_csv(m.group("cols"))]
        selects = _split_csv(m.group("select"))
        try:
            idx = cols.index(target_col.lower())
            if idx < len(selects):
                exprs.append(selects[idx].strip())
        except ValueError:
            continue
    return exprs

def _extract_merge_insert(defn: str, target_col: str) -> List[str]:
    exprs: List[str] = []
    for m in _RE_MERGE_INSERT.finditer(defn):
        cols = [_normalize_brackets(c).lower() for c in _split_csv(m.group("cols"))]
        vals = _split_csv(m.group("vals"))
        try:
            idx = cols.index(target_col.lower())
            if idx < len(vals):
                exprs.append(vals[idx].strip())
        except ValueError:
            continue
    return exprs

# NEW: INSERT ... VALUES (non-MERGE)
def _extract_insert_values(defn: str, target_col: str) -> List[str]:
    exprs: List[str] = []
    for m in _RE_INSERT_VALUES.finditer(defn):
        cols = [_normalize_brackets(c).lower() for c in _split_csv(m.group("cols"))]
        vals = _split_csv(m.group("vals"))
        try:
            idx = cols.index(target_col.lower())
            if idx < len(vals):
                exprs.append(vals[idx].strip())
        except ValueError:
            continue
    return exprs

# NEW: small excerpt helper for highlights
def _excerpt_around(whole: str, needle: str, ctx: int = 160) -> Optional[str]:
    """
    Return a short excerpt around the first occurrence of `needle` in `whole`.
    Case-insensitive; trims to line boundaries.
    """
    if not whole or not needle:
        return None
    try:
        m = re.search(re.escape(needle), whole, re.IGNORECASE)
        if not m:
            return None
        start = max(0, m.start() - ctx)
        end   = min(len(whole), m.end() + ctx)
        ls = whole.rfind("\n", 0, start)
        le = whole.find("\n", end)
        if ls != -1: start = ls + 1
        if le != -1: end = le
        return whole[start:end].strip()
    except Exception:
        return None

# NEW: heuristic for dynamic SQL writers
def _possible_dynamic_write(defn: str, schema: str, table: str, column: str) -> bool:
    s = defn.lower()
    if "sp_executesql" not in s and "exec" not in s:
        return False
    tbl_hint = table.lower() in s or f"{schema.lower()}.{table.lower()}" in s
    col_hint = column.lower() in s
    verbs = any(k in s for k in ["update", "insert", "merge"])
    return tbl_hint and col_hint and verbs

# -------------------------------------------------------------------
# Candidate Discovery (reverse deps + synonyms)
# -------------------------------------------------------------------
def _candidate_procs_for_table(schema: str, table: str) -> List[Dict[str, Any]]:
    with _schema_lock:
        rev = db_schema_cache.get("rev_deps", {})
        procs = db_schema_cache.get("procedures", {})
        syn_by_base = db_schema_cache.get("synonyms_by_base", {})

        keys = set([(schema.lower(), table.lower())])
        for syn_schema, syn_name in syn_by_base.get((schema.lower(), table.lower()), []):
            keys.add((syn_schema.lower(), syn_name.lower()))

        proc_ids = set()
        for k in keys:
            proc_ids |= set(rev.get(k, set()))
        return [procs[pid] for pid in proc_ids if pid in procs]

def _scan_procs_for_writes(procs: List[Dict[str, Any]], schema: str, table: str, column: str,
                           include_definitions: str) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    for p in procs:
        defn = p.get("definition", "") or ""
        if not defn:
            continue
        dlow = defn.lower()
        if (column.lower() not in dlow) or (table.lower() not in dlow and f"{schema.lower()}.{table.lower()}" not in dlow):
            if ("insert" not in dlow) and ("update" not in dlow) and ("merge" not in dlow):
                continue

        exprs: List[str] = []
        exprs.extend(_extract_update_sets(defn, column))
        exprs.extend(_extract_insert_select(defn, column))
        exprs.extend(_extract_merge_update_sets(defn, column))
        exprs.extend(_extract_merge_insert(defn, column))
        exprs.extend(_extract_insert_values(defn, column))   # NEW: INSERT ... VALUES
        # If nothing matched, consider dynamic SQL heuristic
        if not exprs and _possible_dynamic_write(defn, schema, table, column):
            item = {
                "object_id": p["object_id"],
                "schema": p["schema"],
                "name": p["name"],
                "expressions": [],
                "dynamic_sql_suspected": True
            }
            snip = _excerpt_around(defn, "sp_executesql", ctx=160) or _excerpt_around(defn, "exec", ctx=160)
            if snip:
                item["snippet"] = snip
            if include_definitions in ("excerpt", "full"):
                item["text"] = {"kind": "excerpt", "max": 2000, "content": defn[:2000]}
            if include_definitions == "full":
                item["text"] = {"kind": "full", "content": defn}
            results.append(item)
            continue

        if not exprs:
            continue

        item: Dict[str, Any] = {
            "object_id": p["object_id"],
            "schema": p["schema"],
            "name": p["name"],
            "expressions": list(dict.fromkeys(exprs)),
        }

        # Always attach targeted highlights (short excerpts) around matched expressions
        highlights = []
        for e in item["expressions"]:
            snip = _excerpt_around(defn, e, ctx=160)
            if snip:
                highlights.append({"expression": e, "excerpt": snip})
        if highlights:
            item["highlights"] = highlights
            item["snippet"] = highlights[0]["excerpt"]  # convenience: first one

        # Optional: return body excerpts/full text based on include_definitions
        if include_definitions in ("excerpt", "full"):
            item["text"] = {"kind": "excerpt", "max": 2000, "content": defn[:2000]}
        if include_definitions == "full":
            item["text"] = {"kind": "full", "content": defn}

        results.append(item)
    return results

def _find_writing_procs(schema: str, table: str, column: str, include_definitions: str = "none") -> List[Dict[str, Any]]:
    candidates = _candidate_procs_for_table(schema, table)
    results = _scan_procs_for_writes(candidates, schema, table, column, include_definitions)
    if results:
        return results
    # Fallback: scan all cached procedures (cap)
    with _schema_lock:
        procs_src = list(db_schema_cache.get("procedures", {}).values())
    if len(procs_src) > MAX_PROC_SCAN:
        procs_src = procs_src[:MAX_PROC_SCAN]
    return _scan_procs_for_writes(procs_src, schema, table, column, include_definitions)

# -------------------------------------------------------------------
# Extra writers & column metadata
# -------------------------------------------------------------------
def _trigger_writers(schema: str, table: str, column: str, include_definitions: str) -> List[Dict[str, Any]]:
    writers: List[Dict[str, Any]] = []
    tbl_id = _object_id(schema, table)
    if not tbl_id:
        return writers
    with metadata_cursor() as cursor:
        try:
            cursor.execute("""
                SELECT t.object_id,
                       OBJECT_SCHEMA_NAME(t.object_id) AS trig_schema,
                       OBJECT_NAME(t.object_id)  AS trig_name,
                       m.definition
                FROM sys.triggers t
                JOIN sys.sql_modules m ON m.object_id = t.object_id
                WHERE t.parent_id = ?
            """, tbl_id)
            rows = cursor.fetchall()
        except Exception:
            rows = []

    for r in rows:
        defn = (r.definition or "")
        if not defn:
            continue
        exprs = []
        exprs.extend(_extract_update_sets(defn, column))
        exprs.extend(_extract_merge_update_sets(defn, column))
        if not exprs:
            continue
        item = {
            "object_id": r.object_id,
            "schema": r.trig_schema,
            "name": r.trig_name,
            "kind": "trigger",
            "expressions": list(dict.fromkeys(exprs)),
        }
        highlights = []
        for e in item["expressions"]:
            snip = _excerpt_around(defn, e, ctx=160)
            if snip:
                highlights.append({"expression": e, "excerpt": snip})
        if highlights:
            item["highlights"] = highlights
            item["snippet"] = highlights[0]["excerpt"]
        if include_definitions in ("excerpt", "full"):
            item["text"] = {"kind": "excerpt", "max": 2000, "content": defn[:2000]}
        if include_definitions == "full":
            item["text"] = {"kind": "full", "content": defn}
        writers.append(item)
    return writers

def _computed_column_definition(schema: str, table: str, column: str) -> Optional[str]:
    with metadata_cursor() as cursor:
        try:
            cursor.execute("""
                SELECT cc.definition
                FROM sys.computed_columns cc
                JOIN sys.columns c ON c.object_id = cc.object_id AND c.column_id = cc.column_id
                WHERE cc.object_id = OBJECT_ID(QUOTENAME(?) + '.' + QUOTENAME(?))
                  AND c.name = ?
            """, schema, table, column)
            r = cursor.fetchone()
            return r.definition if r else None
        except Exception:
            return None

def _default_constraint_definition(schema: str, table: str, column: str) -> Optional[str]:
    with metadata_cursor() as cursor:
        try:
            cursor.execute("""
                SELECT dc.definition
                FROM sys.columns c
                JOIN sys.default_constraints dc ON dc.object_id = c.default_object_id
                WHERE c.object_id = OBJECT_ID(QUOTENAME(?) + '.' + QUOTENAME(?))
                  AND c.name = ?
            """, schema, table, column)
            r = cursor.fetchone()
            return r.definition if r else None
        except Exception:
            return None

# -------------------------------------------------------------------
# Name preference & rowcount helpers (for disambiguation)
# -------------------------------------------------------------------
_NAME_PREFS = [
    (re.compile(r"\bemployees?\b", re.I), 3),
    (re.compile(r"\bemployee[_ ]?master\b", re.I), 3),
    (re.compile(r"\bpayroll\b", re.I), 2),
    (re.compile(r"\bcomp(ensation)?\b", re.I), 1),
]

def _name_preference_score(fq_table: str) -> int:
    base = fq_table.split(".", 1)[-1]
    for rx, score in _NAME_PREFS:
        if rx.search(base):
            return score
    return 0

def _table_row_count(schema: str, table: str) -> int:
    """Approximate rowcount using sys.partitions (works for most perms)."""
    with metadata_cursor() as c:
        try:
            c.execute("""
                SELECT SUM(p.rows)
                FROM sys.tables t
                JOIN sys.schemas s ON s.schema_id = t.schema_id
                JOIN sys.partitions p ON p.object_id = t.object_id
                WHERE s.name = ? AND t.name = ? AND p.index_id IN (0,1)
            """, schema, table)
            r = c.fetchone()
            return int(r[0]) if r and r[0] is not None else 0
        except Exception:
            return 0

# -------------------------------------------------------------------
# MCP Server
# -------------------------------------------------------------------
mcp = FastMCP("DOTA — Data Origin & Traceability Assistant")

def _startup():
    try:
        counts = load_schema_cache()
        logger.info("Schema cache loaded: %s", counts)
    except Exception as e:
        logger.warning("Schema cache load failed: %s", e)

# -------------------------------------------------------------------
# Core Tools (connection + schema)
# -------------------------------------------------------------------
@mcp.tool
def refresh_schema() -> Dict[str, object]:
    _lineage_core.cache_clear()
    return {"success": True, **load_schema_cache()}

@mcp.tool
def current_connection() -> Dict[str, object]:
    with _config_lock:
        return {
            "success": True,
            "server": DB_CONFIG.get("server"),
            "database": DB_CONFIG.get("database"),
            "driver": DB_CONFIG.get("driver"),
            "timeout": DB_CONFIG.get("timeout"),
            "login_timeout": DB_CONFIG.get("login_timeout"),
            "username": DB_CONFIG.get("username"),
        }

@mcp.tool
def test_connection(
    server: str,
    database: str,
    username: str,
    password: str,
    driver: Optional[str] = None,
    timeout: Optional[int] = None,
    login_timeout: Optional[int] = None,
) -> Dict[str, object]:
    proposed = {
        "server": server,
        "database": database,
        "username": username,
        "password": password,
        "driver": driver or DB_CONFIG.get("driver", "{ODBC Driver 17 for SQL Server}"),
        "timeout": int(timeout if timeout is not None else DB_CONFIG.get("timeout", 30)),
        "login_timeout": int(login_timeout if login_timeout is not None else DB_CONFIG.get("login_timeout", 15)),
    }
    ok, err = _test_connection(proposed)
    return {"success": ok, "error": err} if not ok else {"success": True}

@mcp.tool
def connect_db(
    server: str,
    database: str,
    username: str,
    password: str,
    driver: Optional[str] = None,
    timeout: Optional[int] = None,
    login_timeout: Optional[int] = None,
    persist_to_env: bool = False,
    persist_mask_password: bool = False,
) -> Dict[str, object]:
    """
    Switch the active SQL Server connection.
    Required: server, database, username, password.
    Optional: driver, timeout, login_timeout.
    If persist_to_env=True, updates .env keys. If persist_mask_password=True, writes '***' instead of the real password.
    """
    result = set_db_config(server, database, username, password, driver, timeout, login_timeout)
    if persist_to_env:
        env_path = os.getenv("DOTENV_PATH", ".env")
        try:
            set_key(env_path, "DB_SERVER", server)
            set_key(env_path, "DB_NAME", database)
            set_key(env_path, "DB_USER", username)
            set_key(env_path, "DB_PASS", "***" if persist_mask_password else password)
            if driver:
                set_key(env_path, "DB_DRIVER", driver)
            if timeout is not None:
                set_key(env_path, "DB_TIMEOUT", str(timeout))
            if login_timeout is not None:
                set_key(env_path, "DB_LOGIN_TIMEOUT", str(login_timeout))
            result["persisted"] = True
            result["env_file"] = env_path
        except Exception as e:
            result["persisted"] = False
            result["persist_error"] = str(e)
    return result

@mcp.tool
def list_env_defaults() -> Dict[str, object]:
    return {
        "success": True,
        "DB_SERVER": os.getenv("DB_SERVER"),
        "DB_NAME": os.getenv("DB_NAME"),
        "DB_USER": os.getenv("DB_USER"),
        "DB_DRIVER": os.getenv("DB_DRIVER"),
        "DB_TIMEOUT": os.getenv("DB_TIMEOUT"),
        "DB_LOGIN_TIMEOUT": os.getenv("DB_LOGIN_TIMEOUT"),
    }

@mcp.tool
def permissions_self_test() -> Dict[str, object]:
    """
    Best-effort checks for required metadata visibility.
    Returns booleans for common privileges that impact lineage detail.
    """
    ok_modules = ok_deps = ok_computed = ok_defaults = True
    reasons = {}
    try:
        with metadata_cursor() as c:
            c.execute("SELECT TOP 1 definition FROM sys.sql_modules")
            _ = c.fetchone()
    except Exception as e:
        ok_modules = False
        reasons["sql_modules"] = str(e)

    try:
        with metadata_cursor() as c:
            c.execute("SELECT TOP 1 * FROM sys.sql_expression_dependencies")
            _ = c.fetchone()
    except Exception as e:
        ok_deps = False
        reasons["sql_expression_dependencies"] = str(e)

    try:
        with metadata_cursor() as c:
            c.execute("SELECT TOP 1 * FROM sys.computed_columns")
            _ = c.fetchone()
    except Exception as e:
        ok_computed = False
        reasons["computed_columns"] = str(e)

    try:
        with metadata_cursor() as c:
            c.execute("SELECT TOP 1 * FROM sys.default_constraints")
            _ = c.fetchone()
    except Exception as e:
        ok_defaults = False
        reasons["default_constraints"] = str(e)

    return {
        "success": True,
        "visibility": {
            "sys.sql_modules": ok_modules,
            "sys.sql_expression_dependencies": ok_deps,
            "sys.computed_columns": ok_computed,
            "sys.default_constraints": ok_defaults,
        },
        "notes": reasons,
    }

# -------------------------------------------------------------------
# Schema/data/object misc tools
# -------------------------------------------------------------------
@mcp.tool
def get_table_schema(table: str) -> Dict[str, object]:
    (schema, table_name), _ = validate_table_column(table)
    with metadata_cursor() as cursor:
        cursor.execute("""
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
        """, schema, table_name)
        rows = cursor.fetchall()
        columns = [{"COLUMN_NAME": r.COLUMN_NAME, "DATA_TYPE": r.DATA_TYPE, "IS_NULLABLE": r.IS_NULLABLE} for r in rows]
    return {"success": True, "schema": schema, "table": table_name, "columns": columns}

@mcp.tool
def get_column_data(table: str, select_col: str, where_col: str, value: str) -> Dict[str, object]:
    (schema, table_name), _ = validate_table_column(table)
    _, select_col_real = validate_table_column(f"{schema}.{table_name}", select_col)
    _, where_col_real = validate_table_column(f"{schema}.{table_name}", where_col)
    query = f"SELECT TOP 20 [{select_col_real}] FROM [{schema}].[{table_name}] WHERE [{where_col_real}] = ?"
    with db_cursor() as cursor:
        cursor.execute(query, value)
        rows = cursor.fetchall()
    return {"success": True, "schema": schema, "table": table_name, "results": [row[0] for row in rows]}

# --- Robust object definition (qualified/unqualified; cache-cold safe) ---
@mcp.tool
def get_object_definition(object: str) -> Dict[str, object]:
    """
    Returns the CREATE/ALTER text for a procedure/view/function/etc.
    Accepts qualified ('dbo.Proc') or unqualified ('Proc') names.
    Works even if the objects cache wasn't warmed.
    """
    name = object.strip()
    lookup_keys = [name.lower()]
    bare = name
    if "." in name:
        schema, obj = name.split(".", 1)
        bare = obj
        lookup_keys.append(obj.lower())

    resolved = None
    with _schema_lock:
        obj_cache = db_schema_cache.get("objects", {}) or {}
        for k in lookup_keys:
            resolved = obj_cache.get(k)
            if resolved:
                break

    # If cache miss, try direct resolution paths
    if not resolved:
        with metadata_cursor() as cursor:
            # 1) Try the provided name as-is (qualified or bare)
            cursor.execute("SELECT OBJECT_DEFINITION(OBJECT_ID(?)) AS definition", name)
            row = cursor.fetchone()
            if row and row.definition:
                return {"success": True, "object": name, "definition": row.definition}

            # 2) If bare, try to find its schema and then resolve
            cursor.execute("""
                SELECT TOP 1 OBJECT_SCHEMA_NAME(object_id) AS s, name
                FROM sys.objects
                WHERE name = ?
                ORDER BY object_id DESC
            """, bare)
            r = cursor.fetchone()
            if r:
                qname = f"{r.s}.{r.name}"
                cursor.execute("SELECT OBJECT_DEFINITION(OBJECT_ID(?)) AS definition", qname)
                row = cursor.fetchone()
                if row and row.definition:
                    return {"success": True, "object": qname, "definition": row.definition}

        # Still not found
        raise ValueError("Object not found.")

    # Cache hit → use the resolved qualified name
    with metadata_cursor() as cursor:
        cursor.execute("SELECT OBJECT_DEFINITION(OBJECT_ID(?)) AS definition", resolved)
        row = cursor.fetchone()
    return {"success": True, "object": resolved, "definition": row.definition if row else None}

# -------------------------------------------------------------------
# Jobs overview — internal impl + wrappers
# -------------------------------------------------------------------
def _get_jobs_overview_impl(
    job_name: Optional[str] = None,
    include_running: bool = True,
    failure_lookback_days: int = 30,
    limit: Optional[int] = None
) -> Dict[str, object]:
    params: List[Any] = []
    job_filter_sql = ""
    if job_name:
        job_filter_sql = "WHERE j.name = ?"
        params.append(job_name)

    cutoff = int((datetime.utcnow() - timedelta(days=int(failure_lookback_days))).strftime("%Y%m%d"))

    with metadata_cursor() as c:
        # Running jobs (best-effort)
        running_ids = set()
        if include_running:
            try:
                c.execute("SELECT TOP 1 session_id FROM msdb.dbo.syssessions ORDER BY agent_start_date DESC")
                sess = c.fetchone()
                if sess:
                    c.execute("""
                        SELECT j.job_id
                        FROM msdb.dbo.sysjobactivity a
                        JOIN msdb.dbo.sysjobs j ON j.job_id = a.job_id
                        WHERE a.session_id = ?
                          AND a.start_execution_date IS NOT NULL
                          AND a.stop_execution_date IS NULL
                    """, sess.session_id)
                    running_ids = {row.job_id for row in c.fetchall()}
            except Exception:
                running_ids = set()

        # Latest outcome per job
        c.execute(f"""
            WITH last_hist AS (
                SELECT j.job_id, j.name,
                       MAX(CASE WHEN h.step_id = 0 THEN
                           CONVERT(datetime,
                               STUFF(STUFF(RIGHT('000000'+CAST(h.run_date AS VARCHAR(8)),8),5,0,'-'),8,0,'-') + ' ' +
                               STUFF(STUFF(RIGHT('000000'+CAST(h.run_time AS VARCHAR(6)),6),3,0,':'),6,0,':')
                           ) END) AS last_run_dt
                FROM msdb.dbo.sysjobs j
                LEFT JOIN msdb.dbo.sysjobhistory h ON j.job_id = h.job_id
                {job_filter_sql}
                GROUP BY j.job_id, j.name
            )
            SELECT lr.job_id, lr.name,
                   MAX(CASE WHEN h.step_id = 0 THEN
                        CASE h.run_status
                          WHEN 0 THEN 'Failed'
                          WHEN 1 THEN 'Succeeded'
                          WHEN 2 THEN 'Retry'
                          WHEN 3 THEN 'Canceled'
                          WHEN 4 THEN 'InProgress'
                          ELSE 'Unknown'
                        END
                   END) AS last_status,
                   MAX(CASE WHEN h.step_id = 0 THEN
                        CONVERT(datetime,
                           STUFF(STUFF(RIGHT('000000'+CAST(h.run_date AS VARCHAR(8)),8),5,0,'-'),8,0,'-') + ' ' +
                           STUFF(STUFF(RIGHT('000000'+CAST(h.run_time AS VARCHAR(6)),6),3,0,':'),6,0,':')
                        )
                   END) AS last_run_dt
            FROM last_hist lr
            LEFT JOIN msdb.dbo.sysjobhistory h ON h.job_id = lr.job_id
            GROUP BY lr.job_id, lr.name
        """, *params)
        jobs = c.fetchall()

        # Latest failure per job
        failure_map: Dict[Any, List[Dict[str, Any]]] = {}
        if failure_lookback_days and failure_lookback_days > 0:
            c.execute(f"""
                SELECT j.job_id, j.name AS job_name, h.instance_id, h.run_date, h.run_time, h.message
                FROM msdb.dbo.sysjobs j
                JOIN msdb.dbo.sysjobhistory h ON j.job_id = h.job_id
                WHERE h.run_status = 0
                  AND h.step_id = 0
                  AND h.run_date >= ?
                  {('AND j.name = ?' if job_name else '')}
            """, *( [cutoff] + ([job_name] if job_name else []) ))
            fails = c.fetchall()

            for f in fails:
                try:
                    when = c.execute("""
                        SELECT CONVERT(datetime,
                           STUFF(STUFF(RIGHT('000000'+CAST(? AS VARCHAR(8)),8),5,0,'-'),8,0,'-') + ' ' +
                           STUFF(STUFF(RIGHT('000000'+CAST(? AS VARCHAR(6)),6),3,0,':'),6,0,':')
                        )
                    """, f.run_date, f.run_time).fetchone()[0]
                except Exception:
                    when = None
                failure_map.setdefault(f.job_id, []).append({
                    "job": f.job_name,
                    "failed_at": when.isoformat() if when else None,
                    "summary_message": f.message,
                    "step_id": None,
                    "step_name": None,
                    "step_message": None,
                })

            # Attach best-effort failing step per job
            for job_id in list(failure_map.keys()):
                try:
                    c.execute("""
                        SELECT TOP 1 h.step_id, s.step_name, h.message AS step_message
                        FROM msdb.dbo.sysjobhistory h
                        LEFT JOIN msdb.dbo.sysjobsteps s
                               ON s.job_id = h.job_id AND s.step_id = h.step_id
                        WHERE h.job_id = ?
                          AND h.run_status = 0
                          AND h.step_id > 0
                        ORDER BY h.instance_id DESC, h.step_id DESC
                    """, job_id)
                    fr = c.fetchone()
                    if fr and failure_map[job_id]:
                        failure_map[job_id][0]["step_id"] = fr.step_id
                        failure_map[job_id][0]["step_name"] = fr.step_name
                        failure_map[job_id][0]["step_message"] = fr.step_message
                except Exception:
                    continue

    items = []
    for r in jobs:
        jid = r.job_id
        entry = {
            "job": r.name,
            "status": r.last_status or "Unknown",
            "last_run": r.last_run_dt.isoformat() if getattr(r, "last_run_dt", None) else None,
            "running": (jid in running_ids),
            "last_failure": None
        }
        fl = failure_map.get(jid, [])
        if fl:
            fl.sort(key=lambda x: (x["failed_at"] or ""), reverse=True)
            entry["last_failure"] = fl[0]
        items.append(entry)

    items.sort(key=lambda x: x["job"].lower())
    if limit is not None:
        items = items[:int(limit)]

    return {"success": True, "count": len(items), "jobs": items}

@mcp.tool
def get_jobs_overview(
    job_name: Optional[str] = None,
    include_running: bool = True,
    failure_lookback_days: int = 30,
    limit: Optional[int] = None
) -> Dict[str, object]:
    return _get_jobs_overview_impl(job_name, include_running, failure_lookback_days, limit)

@mcp.tool
def ask_jobs(prompt: str,
             failure_lookback_days: int = 30,
             include_running: bool = True,
             limit: Optional[int] = None) -> Dict[str, object]:
    """
    NL wrapper:
      - "what are the status of the jobs?"
      - "what is the status of job X?"
      - "what is the reason of failure of job X?"
      - "show failures for job X in last 7 days"
    """
    p = prompt.strip().lower()
    job_name = None
    m = re.search(r"(?:of|for)?\s*job\s+(.+)$", p)
    if m:
        job_name = re.sub(r"[?.!]\s*$", "", prompt[m.start(1):].strip())

    m2 = re.search(r"last\s+(\d+)\s+days", p)
    if m2:
        failure_lookback_days = int(m2.group(1))

    wants_failure = any(k in p for k in ["reason of failure", "why failed", "failures", "failed", "failure"])
    res = _get_jobs_overview_impl(
        job_name=job_name,
        include_running=include_running,
        failure_lookback_days=failure_lookback_days,
        limit=limit
    )

    if not res.get("success"):
        return res

    if wants_failure:
        jobs = res.get("jobs", [])
        if job_name:
            jobs = [j for j in jobs if j["job"].lower() == job_name.lower()]
        failures = []
        for j in jobs:
            lf = j.get("last_failure")
            if lf:
                failures.append({
                    "job": j["job"],
                    "failed_at": lf.get("failed_at"),
                    "step_id": lf.get("step_id"),
                    "step_name": lf.get("step_name"),
                    "message": lf.get("step_message") or lf.get("summary_message")
                })
        return {"success": True, "failures": failures, "lookback_days": failure_lookback_days}

    return res

# -------------------------------------------------------------------
# Lineage core — NO Graphviz, returns logic graph
# -------------------------------------------------------------------
def _effective_depth(max_depth: Optional[int]) -> int:
    depth = DEFAULT_LINEAGE_MAX_DEPTH if (max_depth is None) else int(max_depth)
    if depth > MAX_ALLOWED_LINEAGE_DEPTH:
        raise ValueError(f"max_depth too high — choose {MAX_ALLOWED_LINEAGE_DEPTH} or less.")
    if depth < 1:
        depth = 1
    return depth

@lru_cache(maxsize=512)
def _lineage_core(server: str, database: str, table: str, column: str,
                  depth: int, defs_mode: str) -> Dict[str, Any]:
    schema, table_name = _get_table_schema_and_name(table)
    target_node = f"{schema}.{table_name}:{column}"

    nodes: Dict[str, Dict[str, Any]] = {}
    edges: List[Dict[str, Any]] = []
    mappings: List[Dict[str, Any]] = []

    # target column node
    nodes[target_node] = {"type": "column", "schema": schema, "name": table_name, "column": column}

    # computed/default
    computed = _computed_column_definition(schema, table_name, column)
    default_def = _default_constraint_definition(schema, table_name, column)

    # writer procedures
    writers = _find_writing_procs(schema, table_name, column, include_definitions=defs_mode)
    for proc in writers:
        proc_node = f"{proc['schema']}.{proc['name']}"
        nodes[proc_node] = {"type": "procedure", "schema": proc["schema"], "name": proc["name"]}
        edges.append({"source": proc_node, "target": target_node, "relation": "writes"})
        for e in proc.get("expressions", []):
            mappings.append({
                "target": {"schema": schema, "table": table_name, "column": column},
                "proc": {"schema": proc["schema"], "name": proc["name"]},
                "expression": e,
            })

    # writer triggers
    trig_writers = _trigger_writers(schema, table_name, column, include_definitions=defs_mode)
    for trg in trig_writers:
        trg_node = f"{trg['schema']}.{trg['name']}"
        nodes[trg_node] = {"type": "trigger", "schema": trg["schema"], "name": trg["name"]}
        edges.append({"source": trg_node, "target": target_node, "relation": "writes"})
        for e in trg.get("expressions", []):
            mappings.append({
                "target": {"schema": schema, "table": table_name, "column": column},
                "proc": {"schema": trg["schema"], "name": trg["name"]},
                "expression": e,
            })

    # BFS upstream via dependencies (procedures only)
    queue: List[Tuple[int, int, str]] = []  # (object_id, depth, via_proc_node)
    seen: set = set()
    for item in writers:
        obj_id = item.get("object_id")
        if obj_id:
            via_node = f"{item['schema']}.{item['name']}"
            queue.append((obj_id, 1, via_node))

    while queue:
        obj_id, d, via_proc_node = queue.pop(0)
        if d >= depth or obj_id in seen:
            continue
        seen.add(obj_id)

        with metadata_cursor() as cursor:
            try:
                cursor.execute("""
                    SELECT
                        d.referenced_id,
                        OBJECT_SCHEMA_NAME(d.referenced_id) AS ref_schema,
                        OBJECT_NAME(d.referenced_id) AS ref_name,
                        o.[type] AS ref_type
                    FROM sys.sql_expression_dependencies d
                    LEFT JOIN sys.objects o ON o.object_id = d.referenced_id
                    WHERE d.referencing_id = ?
                """, obj_id)
                deps = cursor.fetchall()
            except Exception:
                deps = []

        for dep in deps:
            if not dep.ref_name:
                continue
            node_id = f"{dep.ref_schema}.{dep.ref_name}"
            if node_id not in nodes:
                nodes[node_id] = {"type": dep.ref_type, "schema": dep.ref_schema, "name": dep.ref_name}
            edges.append({"source": node_id, "target": via_proc_node, "relation": "feeds"})
            if dep.ref_type == 'P':
                ref_oid = _object_id(dep.ref_schema, dep.ref_name)
                if ref_oid:
                    queue.append((ref_oid, d + 1, node_id))

    # dedupe edges
    uniq = []
    seen_e = set()
    for e in edges:
        k = (e["source"], e["target"], e.get("relation", ""))
        if k in seen_e:
            continue
        seen_e.add(k)
        uniq.append(e)

    return {
        "success": True,
        "target": {"schema": schema, "table": table_name, "column": column},
        "graph": {"nodes": nodes, "edges": uniq},
        "candidate_population_expressions": mappings,
        "writer_procedures": writers,
        "writer_triggers": trig_writers,
        "computed_column": computed,
        "default_constraint": default_def,
    }

def _get_column_lineage_impl(
    table: str,
    column: str,
    max_depth: Optional[int] = None,
    include_definitions: str = "none",
) -> Dict[str, object]:
    if include_definitions not in ("none", "excerpt", "full"):
        raise ValueError("include_definitions must be: none | excerpt | full")
    depth = _effective_depth(max_depth)

    with _config_lock:
        server = str(DB_CONFIG.get("server") or "")
        database = str(DB_CONFIG.get("database") or "")
    res = _lineage_core(server, database, table, column, depth, include_definitions)

    provenance = {"database": database} if EXPOSE_DATABASE_ONLY else None

    out = {
        "success": True,
        "effective_max_depth": depth,
        **res,
        "notes": [
            "Expressions parsed best-effort for INSERT/UPDATE/MERGE; dynamic SQL may not be detected.",
            "Dependencies from sys.sql_expression_dependencies require VIEW DEFINITION permission.",
            f"Depth limited by MAX_ALLOWED_LINEAGE_DEPTH={MAX_ALLOWED_LINEAGE_DEPTH}.",
        ],
    }
    if provenance:
        out["provenance"] = provenance
    return out

def _get_column_population_impl(
    table: str,
    column: str,
    max_depth: Optional[int] = None,
    include_definitions: str = "none",
) -> Dict[str, object]:
    result = _get_column_lineage_impl(
        table=table,
        column=column,
        max_depth=max_depth,
        include_definitions=include_definitions,
    )
    if not result.get("success"):
        return result

    gnodes: Dict[str, Dict[str, Any]] = result["graph"]["nodes"]
    gedges: List[Dict[str, Any]] = result["graph"]["edges"]

    topo_nodes: List[Dict[str, Any]] = []
    for key, meta in gnodes.items():
        ntype = meta.get("type")
        if ntype == "column":
            label = f"{meta.get('schema')}.{meta.get('name')}.{meta.get('column')}"
        else:
            label = f"{meta.get('schema')}.{meta.get('name')}"
        topo_nodes.append({
            "id": key,
            "label": label,
            "type": ntype,
            "schema": meta.get("schema"),
            "name": meta.get("name"),
            "column": meta.get("column"),
        })

    seen = set()
    topo_edges: List[Dict[str, Any]] = []
    for e in gedges:
        src, dst = e["source"], e["target"]
        rel = e.get("relation", "")
        k = (src, dst, rel)
        if k in seen:
            continue
        seen.add(k)
        topo_edges.append({
            "from": src,
            "to": dst,
            "relation": rel,
            "label": "writes" if rel == "writes" else ""
        })

    # Convenience: small list of writer snippets (also present on each writer)
    writer_snippets = []
    for w in result.get("writer_procedures", []):
        if w.get("snippet"):
            writer_snippets.append({
                "procedure": f"{w['schema']}.{w['name']}",
                "snippet": w["snippet"]
            })

    out = {
        "success": True,
        "target": result["target"],
        "topology": {"nodes": topo_nodes, "edges": topo_edges},
        "population": {
            "computed_column": result.get("computed_column"),
            "default_constraint": result.get("default_constraint"),
            "writer_procedures": result.get("writer_procedures", []),
            "writer_triggers": result.get("writer_triggers", []),
            "candidate_population_expressions": result.get("candidate_population_expressions", []),
            "writer_snippets": writer_snippets,
        },
        "effective_max_depth": result.get("effective_max_depth"),
        "notes": result.get("notes", []),
    }
    if "provenance" in result and result["provenance"]:
        out["provenance"] = result["provenance"]
    return out

# ---------- Column finder impl ----------
def _find_tables_with_column_impl(
    column: str,
    case_insensitive: bool = True,
    include_views: bool = False
) -> Dict[str, object]:
    col_key = column.lower() if case_insensitive else column

    # 1) Try cache
    with _schema_lock:
        col_index = db_schema_cache.get("columns_index") or {}
        hits = list(col_index.get(col_key, []))

    # 2) If cache empty or not found, query catalog and warm cache
    if not hits:
        with metadata_cursor() as c:
            if include_views:
                c.execute("""
                    SELECT TABLE_SCHEMA, TABLE_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE {0} = ?
                    ORDER BY TABLE_SCHEMA, TABLE_NAME
                """.format("LOWER(COLUMN_NAME)" if case_insensitive else "COLUMN_NAME"),
                col_key if case_insensitive else column)
            else:
                c.execute("""
                    SELECT C.TABLE_SCHEMA, C.TABLE_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS C
                    JOIN INFORMATION_SCHEMA.TABLES T
                      ON T.TABLE_SCHEMA = C.TABLE_SCHEMA AND T.TABLE_NAME = C.TABLE_NAME
                    WHERE T.TABLE_TYPE = 'BASE TABLE'
                      AND {0} = ?
                    ORDER BY C.TABLE_SCHEMA, C.TABLE_NAME
                """.format("LOWER(COLUMN_NAME)" if case_insensitive else "COLUMN_NAME"),
                col_key if case_insensitive else column)
            rows = c.fetchall()
            hits = [f"{r.TABLE_SCHEMA}.{r.TABLE_NAME}" for r in rows]

        with _schema_lock:
            db_schema_cache.setdefault("columns_index", {}).setdefault(col_key, [])
            cur = set(db_schema_cache["columns_index"][col_key])
            db_schema_cache["columns_index"][col_key] = sorted(cur | set(hits))

    return {"success": True, "column": column, "tables": hits, "count": len(hits)}

# -------------------------------------------------------------------
# Public Tools — lineage + population + finders + NL wrappers
# -------------------------------------------------------------------
@mcp.tool
def get_column_lineage(
    table: str,
    column: str,
    max_depth: Optional[int] = None,
    include_definitions: str = DEFAULT_INCLUDE_DEFS,
) -> Dict[str, object]:
    """Canonical lineage (logic only)."""
    return _get_column_lineage_impl(table, column, max_depth, include_definitions)

@mcp.tool
def get_column_population(
    table: str,
    column: str,
    max_depth: Optional[int] = None,
    include_definitions: str = DEFAULT_INCLUDE_DEFS,   # default shows short proc text & highlights
) -> Dict[str, object]:
    """Lineage + topology for client-side rendering."""
    return _get_column_population_impl(table, column, max_depth, include_definitions)

@mcp.tool
def find_tables_with_column(
    column: str,
    case_insensitive: bool = True,
    include_views: bool = False
) -> Dict[str, object]:
    return _find_tables_with_column_impl(column, case_insensitive, include_views)

@mcp.tool
def ask_where_column(prompt: str) -> Dict[str, object]:
    """
    NL: "which table has column salary?", "where is Salary column?", etc.
    """
    p = prompt.strip()
    m = re.search(r"(?:table\s+.*\.)?column\s+([A-Za-z0-9_]+)", p, re.I)
    if not m:
        m = re.search(r"(?:which\s+table\s+has\s+)?([A-Za-z0-9_]+)\s*(?:column)?", p, re.I)
    if not m:
        return {"success": False, "message": "Please specify the column name, e.g., 'which table has column Salary'."}
    col = m.group(1)
    return _find_tables_with_column_impl(column=col, case_insensitive=True, include_views=False)

@mcp.tool
def ask_column_population(
    prompt: str,
    max_depth: Optional[int] = None,
    include_definitions: str = DEFAULT_INCLUDE_DEFS,   # default shows short proc text & highlights
    table_hints: Optional[List[str]] = None,
    auto_disambiguate: bool = True,
) -> Dict[str, object]:
    """
    NL entrypoint:
      - "how is salary populated?"
      - "how is column Salary populated in table dbo.Employees"
    Tries to auto-select a table; otherwise returns a ranked list.
    """
    if include_definitions not in ("none", "excerpt", "full"):
        return {"success": False, "message": "include_definitions must be: none | excerpt | full"}

    # Direct: "column <col> in table <schema.table>"
    m = re.search(r"column\s+([A-Za-z0-9_]+)\s+in\s+table\s+([A-Za-z0-9_\.]+)", prompt, re.I)
    if m:
        col, table = m.group(1), m.group(2)
        return _get_column_population_impl(table=table, column=col, max_depth=max_depth, include_definitions=include_definitions)

    # Generic: "how is <col> populated"
    m = re.search(r"how\s+is\s+([A-Za-z0-9_]+)\s+populated", prompt, re.I)
    if not m:
        return {"success": False, "message": "Could not parse. Try: 'how is <column> populated' or 'how is column <col> populated in table <schema.table>'."}
    col = m.group(1)
    col_key = col.lower()

    # Ensure cache has candidates; if not, warm from INFORMATION_SCHEMA
    with _schema_lock:
        col_index = db_schema_cache.get("columns_index") or {}
        all_candidates = list(col_index.get(col_key, []))
    if not all_candidates:
        warm = _find_tables_with_column_impl(column=col, case_insensitive=True, include_views=False)
        all_candidates = warm.get("tables", [])

    if not all_candidates:
        return {"success": False, "message": f"Could not find column '{col}' in known tables. Provide the table name, e.g., 'column {col} in table dbo.Employees'."}

    # Apply optional hints
    if table_hints:
        hinted = [t for t in all_candidates if t in table_hints]
        if len(hinted) == 1:
            return _get_column_population_impl(table=hinted[0], column=col, max_depth=max_depth, include_definitions=include_definitions)
        if hinted:
            all_candidates = hinted

    if len(all_candidates) == 1:
        return _get_column_population_impl(table=all_candidates[0], column=col, max_depth=max_depth, include_definitions=include_definitions)

    # Score multi-candidates: (writers+trigs, dbo_pref, name_pref, rowcount)
    scored: List[Tuple[Tuple[int,int,int,int], str]] = []
    for fq in all_candidates:
        try:
            sch, tname = fq.split(".", 1)
        except ValueError:
            sch, tname = _get_table_schema_and_name(fq)
        writers = _find_writing_procs(sch, tname, col, include_definitions="none")
        trigs   = _trigger_writers(sch, tname, col, include_definitions="none")
        wscore  = (len(writers) + len(trigs)) if (writers or trigs) else 0
        dbo_pref = 1 if sch.lower() == "dbo" else 0
        npref   = _name_preference_score(fq)
        rcount  = _table_row_count(sch, tname)
        scored.append(((wscore, dbo_pref, npref, rcount), fq))

    scored.sort(key=lambda x: (-x[0][0], -x[0][1], -x[0][2], -x[0][3], x[1].lower()))
    top_score, top_fq = scored[0][0], scored[0][1]
    second_score = scored[1][0] if len(scored) > 1 else None

    # Auto-pick unless dead tie on all components
    if auto_disambiguate and (second_score is None or top_score != second_score):
        res = _get_column_population_impl(table=top_fq, column=col, max_depth=max_depth, include_definitions=include_definitions)
        res["auto_selected"] = top_fq
        res["alternatives"] = [fq for _, fq in scored[1:4]]
        return res

    # Dead tie: pick first, but include alternatives + note
    if auto_disambiguate and scored:
        res = _get_column_population_impl(table=top_fq, column=col, max_depth=max_depth, include_definitions=include_definitions)
        res["auto_selected"] = top_fq
        res["alternatives"] = [fq for _, fq in scored[1:4]]
        res["tie_break_note"] = "Multiple tables tied; auto-selected the first by name. Alternatives included."
        return res

    # Or return ranked list for the UI to present quick choices
    suggestions = [
        {"table": fq, "writers_plus_triggers": s[0], "dbo_pref": s[1], "name_pref": s[2], "rowcount": s[3]}
        for (s, fq) in scored[:8]
    ]
    return {
        "success": False,
        "message": f"Column '{col}' exists in multiple tables. Select one to continue.",
        "candidates_ranked": suggestions,
        "hint": f"You can also specify: 'how is column {col} populated in table <schema.table>'."
    }

# -------------------------------------------------------------------
# Resources
# -------------------------------------------------------------------
@mcp.resource(
    uri="sql://index",
    description="Overview of SQL metadata: counts and quick links to tables/jobs.",
    mime_type="text/markdown",
)
def resource_index() -> str:
    if not db_schema_cache["tables"] and not db_schema_cache["jobs"]:
        try:
            load_schema_cache()
        except Exception:
            pass
    with _schema_lock:
        tcount = len(db_schema_cache["tables"])
        jcount = len(db_schema_cache["jobs"])
    return (
        "# SQL Metadata Index\n\n"
        f"- **Tables:** {tcount} (see `sql://tables`)\n"
        f"- **Jobs:** {jcount} (see `sql://jobs`)\n\n"
        "Load `sql://tables` or `sql://jobs` to see full lists."
    )

@mcp.resource(
    uri="sql://tables",
    description="Markdown list of all base tables discovered.",
    mime_type="text/markdown",
)
def resource_tables() -> str:
    if not db_schema_cache["tables"]:
        try:
            load_schema_cache()
        except Exception:
            pass
    with _schema_lock:
        tables = sorted(db_schema_cache["tables"].values())
    if not tables:
        return "# Tables\n\n_No tables found in cache. Run the `refresh_schema` tool and try again_."
    lines = ["# Tables", "", f"Total: **{len(tables)}**", ""]
    for t in tables:
        lines.append(f"- {t}")
    return "\n".join(lines)

@mcp.resource(
    uri="sql://jobs",
    description="Markdown list of all SQL Agent jobs discovered.",
    mime_type="text/markdown",
)
def resource_jobs() -> str:
    if not db_schema_cache["jobs"]:
        try:
            load_schema_cache()
        except Exception:
            pass
    with _schema_lock:
        jobs = sorted(db_schema_cache["jobs"].values())
    if not jobs:
        return "# Jobs\n\n_No jobs found in cache. Run the `refresh_schema` tool and try again_."
    lines = ["# Jobs", "", f"Total: **{len(jobs)}**", ""]
    for j in jobs:
        lines.append(f"- {j}")
    return "\n".join(lines)

# -------------------------------------------------------------------
# Entry
# -------------------------------------------------------------------
if __name__ == "__main__":
    _startup()

    use_http = os.getenv("MCP_HTTP", "1") == "1"
    host = os.getenv("MCP_HOST", "127.0.0.1")
    port = int(os.getenv("MCP_PORT", "8000"))

    if use_http:
        logger.info(f"Starting DOTA (FastMCP HTTP) on http://{host}:{port}/mcp/")
        mcp.run(transport="http", host=host, port=port)
    else:
        logger.info("Starting DOTA (FastMCP STDIO)")
        mcp.run()
