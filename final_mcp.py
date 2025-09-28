"""
SQL MCP Tool — FastMCP v2.x Compatible
--------------------------------------
Exposes SQL Server metadata tools via MCP with dynamic connection switching
and best-effort column lineage discovery.

Supports:
- HTTP (default): agent connects at /mcp
- STDIO: fallback if MCP_HTTP=0 (for Claude Desktop etc.)

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

from dotenv import load_dotenv, set_key
import pyodbc
from fastmcp import FastMCP

# -----------------------
# Env & Logging
# -----------------------
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("sql-mcp")

# ---- Lineage depth configuration ----
# Default (can be overridden by env). A *variable*, not hard-coded in the logic paths.
DEFAULT_LINEAGE_MAX_DEPTH = int(os.getenv("LINEAGE_MAX_DEPTH", "2"))
# Hard safety cap to prevent runaway traversals
MAX_ALLOWED_LINEAGE_DEPTH = 10

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
_config_lock = threading.RLock()   # for DB_CONFIG changes
_schema_lock = threading.RLock()   # for schema cache

# -----------------------
# In-memory Schema Cache
# -----------------------
db_schema_cache = {"tables": {}, "columns": {}, "objects": {}, "jobs": {}}

# -----------------------
# DB Helpers
# -----------------------
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

def get_db_connection():
    with _config_lock:
        return pyodbc.connect(_build_conn_str(DB_CONFIG), autocommit=True)

@contextmanager
def db_cursor():
    conn = get_db_connection()
    try:
        yield conn.cursor()
    finally:
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

    # Clear caches on switch
    with _schema_lock:
        db_schema_cache["tables"].clear()
        db_schema_cache["columns"].clear()
        db_schema_cache["objects"].clear()
        db_schema_cache["jobs"].clear()

    counts = load_schema_cache()
    return {"success": True, "connected_to": {"server": server, "database": database}, "schema_counts": counts}

# -----------------------
# Cache Loader
# -----------------------
def load_schema_cache() -> Dict[str, int]:
    with _schema_lock:
        with db_cursor() as cursor:
            cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'")
            tables = [row.TABLE_NAME for row in cursor.fetchall()]
            db_schema_cache["tables"] = {t.lower(): t for t in tables}

            db_schema_cache["columns"] = {}
            for table in tables:
                cursor.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ?", table)
                cols = [row.COLUMN_NAME for row in cursor.fetchall()]
                db_schema_cache["columns"][table.lower()] = {c.lower(): c for c in cols}

            cursor.execute(
                "SELECT ROUTINE_NAME AS obj FROM INFORMATION_SCHEMA.ROUTINES "
                "UNION SELECT TABLE_NAME AS obj FROM INFORMATION_SCHEMA.VIEWS"
            )
            db_schema_cache["objects"] = {row.obj.lower(): row.obj for row in cursor.fetchall()}

            try:
                cursor.execute("SELECT name FROM msdb.dbo.sysjobs")
                db_schema_cache["jobs"] = {row.name.lower(): row.name for row in cursor.fetchall()}
            except Exception:
                db_schema_cache["jobs"] = {}

    return {
        "tables": len(db_schema_cache["tables"]),
        "objects": len(db_schema_cache["objects"]),
        "jobs": len(db_schema_cache["jobs"]),
    }

# -----------------------
# Validators
# -----------------------
def validate_table_column(table: str, column: Optional[str] = None) -> Tuple[str, Optional[str]]:
    with _schema_lock:
        real_table = db_schema_cache["tables"].get(table.lower())
        if not real_table:
            raise ValueError(f"Table '{table}' not found.")
        if column:
            real_column = db_schema_cache["columns"].get(table.lower(), {}).get(column.lower())
            if not real_column:
                raise ValueError(f"Column '{column}' not found in table '{table}'.")
            return real_table, real_column
    return real_table, None

# -----------------------
# Lineage helpers (regex parsers + dependency walkers)
# -----------------------
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
_RE_MERGE = re.compile(
    r"""MERGE\s+(?P<tgt>[\[\]A-Za-z0-9_\.]+)\s+AS\s+\w+.*?WHEN\s+MATCHED\s+THEN\s+UPDATE\s+SET\s+(?P<sets>.+?)(?:WHEN|OUTPUT|;|$)""",
    re.IGNORECASE | re.DOTALL,
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
            parts.append(buf.strip())
            buf = ""
        else:
            buf += ch
    if buf.strip():
        parts.append(buf.strip())
    return parts

def _get_table_schema_and_name(table: str) -> Tuple[str, str]:
    with db_cursor() as cursor:
        # direct match by table
        cursor.execute("""
            SELECT TABLE_SCHEMA, TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE='BASE TABLE' AND LOWER(TABLE_NAME) = LOWER(?)
        """, table)
        row = cursor.fetchone()
        if row:
            return row.TABLE_SCHEMA, row.TABLE_NAME

        # schema-qualified input?
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
    raise ValueError(f"Table '{table}' not found (use schema.table or table).")

def _matches_target_table(defn: str, schema: str, table: str) -> bool:
    simple = table.lower()
    qualified = f"{schema}.{table}".lower()
    d = defn.lower()
    return (qualified in d) or (simple in d)

def _extract_update_sets(defn: str, target_col: str) -> List[str]:
    exprs: List[str] = []
    for m in _RE_UPDATE_SET.finditer(defn):
        sets = m.group("sets")
        for pair in _RE_SET_PAIR.finditer(sets):
            col = _normalize_brackets(pair.group("col")).lower()
            if col == target_col.lower():
                exprs.append(pair.group("expr").strip())
    return exprs

def _extract_merge_sets(defn: str, target_col: str) -> List[str]:
    exprs: List[str] = []
    for m in _RE_MERGE.finditer(defn):
        sets = m.group("sets")
        for pair in _RE_SET_PAIR.finditer(sets):
            col = _normalize_brackets(pair.group("col")).lower()
            if col == target_col.lower():
                exprs.append(pair.group("expr").strip())
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

def _get_proc_dependencies(proc_object_id: int) -> List[Dict[str, Any]]:
    with db_cursor() as cursor:
        cursor.execute("""
            SELECT
                d.referencing_id,
                d.referenced_id,
                OBJECT_SCHEMA_NAME(d.referenced_id) AS ref_schema,
                OBJECT_NAME(d.referenced_id) AS ref_name,
                o.[type] AS ref_type
            FROM sys.sql_expression_dependencies d
            LEFT JOIN sys.objects o ON o.object_id = d.referenced_id
            WHERE d.referencing_id = ?
        """, proc_object_id)
        rows = cursor.fetchall()
        deps = []
        for r in rows:
            deps.append({
                "referenced_id": r.referenced_id,
                "schema": r.ref_schema,
                "name": r.ref_name,
                "type": r.ref_type,  # P=proc, V=view, U=table, FN/IF/TF=function, etc.
            })
        return deps

def _find_writing_procs(schema: str, table: str, column: str) -> List[Dict[str, Any]]:
    like_table_qualified = f"%{schema}.{table}%"
    like_table_simple = f"%{table}%"
    like_col = f"%{column}%"
    with db_cursor() as cursor:
        cursor.execute("""
            SELECT p.object_id, OBJECT_SCHEMA_NAME(p.object_id) AS proc_schema,
                   OBJECT_NAME(p.object_id) AS proc_name, m.definition
            FROM sys.procedures p
            JOIN sys.sql_modules m ON m.object_id = p.object_id
            WHERE (m.definition LIKE ? OR m.definition LIKE ?)
              AND m.definition LIKE ?
              AND (m.definition LIKE '%INSERT%' OR m.definition LIKE '%UPDATE%' OR m.definition LIKE '%MERGE%')
        """, like_table_qualified, like_table_simple, like_col)
        rows = cursor.fetchall()

    procs = []
    for r in rows:
        defn = r.definition or ""
        if not _matches_target_table(defn, schema, table):
            continue
        exprs: List[str] = []
        exprs.extend(_extract_update_sets(defn, column))
        exprs.extend(_extract_insert_select(defn, column))
        exprs.extend(_extract_merge_sets(defn, column))
        procs.append({
            "object_id": r.object_id,
            "schema": r.proc_schema,
            "name": r.proc_name,
            "expressions": list(dict.fromkeys(exprs)),  # dedupe
            "definition_excerpt": defn[:1200],
        })
    return procs

def _obj_node_id(schema: Optional[str], name: str, col: Optional[str] = None) -> str:
    return f"{schema}.{name}:{col}" if (schema and col) else (f"{schema}.{name}" if schema else name)

# -----------------------
# MCP Server
# -----------------------
mcp = FastMCP("SQL MCP Tool")

def _startup():
    try:
        counts = load_schema_cache()
        logger.info("Schema cache loaded: %s", counts)
    except Exception as e:
        logger.warning("Schema cache load failed: %s", e)

# ---- Core Tools ----
@mcp.tool
def refresh_schema() -> Dict[str, object]:
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
            # password intentionally omitted
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
) -> Dict[str, object]:
    """
    Switch the active SQL Server connection.
    Required: server, database, username, password.
    Optional: driver, timeout, login_timeout.
    If persist_to_env=True, updates .env keys DB_SERVER, DB_NAME, DB_USER, DB_PASS, DB_DRIVER.
    """
    result = set_db_config(server, database, username, password, driver, timeout, login_timeout)
    if persist_to_env:
        env_path = os.getenv("DOTENV_PATH", ".env")
        try:
            set_key(env_path, "DB_SERVER", server)
            set_key(env_path, "DB_NAME", database)
            set_key(env_path, "DB_USER", username)
            set_key(env_path, "DB_PASS", password)
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
        # DB_PASS intentionally omitted
    }

# ---- Schema/Data/Jobs/Objects Tools (original set) ----
@mcp.tool
def get_table_schema(table: str) -> Dict[str, object]:
    table_name, _ = validate_table_column(table)
    query = "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ?"
    with db_cursor() as cursor:
        cursor.execute(query, table_name)
        rows = cursor.fetchall()
        columns = [{desc[0]: val for desc, val in zip(cursor.description, row)} for row in rows]
    return {"success": True, "table": table_name, "columns": columns}

@mcp.tool
def get_column_data(table: str, select_col: str, where_col: str, value: str) -> Dict[str, object]:
    table_name, _ = validate_table_column(table)
    _, select_col_real = validate_table_column(table, select_col)
    _, where_col_real = validate_table_column(table, where_col)
    query = f"SELECT TOP 20 {select_col_real} FROM {table_name} WHERE {where_col_real} = ?"
    with db_cursor() as cursor:
        cursor.execute(query, value)
        rows = cursor.fetchall()
    return {"success": True, "results": [row[0] for row in rows]}

@mcp.tool
def get_column_population_logic(column: str) -> Dict[str, object]:
    with db_cursor() as cursor:
        cursor.execute("SELECT ROUTINE_NAME, ROUTINE_DEFINITION FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE='PROCEDURE'")
        matches = []
        for proc in cursor.fetchall():
            definition = getattr(proc, "ROUTINE_DEFINITION", "") or ""
            dlow = definition.lower()
            if column.lower() in dlow and ("insert into" in dlow or "update" in dlow or "merge" in dlow):
                matches.append(proc.ROUTINE_NAME)
    return {"success": True, "column": column, "procedures": matches}

@mcp.tool
def get_object_definition(object: str) -> Dict[str, object]:
    with _schema_lock:
        real_object = db_schema_cache["objects"].get(object.lower())
    if not real_object:
        raise ValueError("Object not found.")
    with db_cursor() as cursor:
        cursor.execute("SELECT OBJECT_DEFINITION(OBJECT_ID(?)) AS definition", real_object)
        row = cursor.fetchone()
    return {"success": True, "object": real_object, "definition": row.definition if row else None}

@mcp.tool
def get_job_status(job: str) -> Dict[str, object]:
    with _schema_lock:
        real_job = db_schema_cache["jobs"].get(job.lower())
    if not real_job:
        raise ValueError("Job not found.")
    query = """
        SELECT TOP 1 j.name, h.run_date, h.run_time,
            CASE h.run_status
                WHEN 0 THEN 'Failed' WHEN 1 THEN 'Succeeded'
                WHEN 2 THEN 'Retry' WHEN 3 THEN 'Canceled'
                ELSE 'Running'
            END AS status
        FROM msdb.dbo.sysjobs j
        LEFT JOIN msdb.dbo.sysjobhistory h ON j.job_id = h.job_id
        WHERE j.name = ? AND h.step_id = 0
        ORDER BY h.run_date DESC, h.run_time DESC
    """
    with db_cursor() as cursor:
        cursor.execute(query, real_job)
        row = cursor.fetchone()
    return (
        {"success": True, "job": row.name, "status": row.status, "last_run_date": row.run_date, "last_run_time": row.run_time}
        if row else {"success": False}
    )

# ---- Lineage Tools ----
@mcp.tool
def get_column_lineage(table: str, column: str, max_depth: Optional[int] = None) -> Dict[str, object]:
    """
    Best-effort lineage for how <table>.<column> is populated.
    - Scans procedures for INSERT/UPDATE/MERGE that write to the column.
    - Extracts RHS expressions mapping to <table>.<column>.
    - Uses sys.sql_expression_dependencies to list upstream objects.
    - Recurses (up to max_depth) to sketch upstream lineage.

    Depth behavior:
      - If max_depth is None, uses DEFAULT_LINEAGE_MAX_DEPTH (env: LINEAGE_MAX_DEPTH, default 2).
      - Regardless, enforces a hard cap of MAX_ALLOWED_LINEAGE_DEPTH (10).
    """
    # Determine effective depth
    depth = DEFAULT_LINEAGE_MAX_DEPTH if (max_depth is None) else int(max_depth)
    if depth > MAX_ALLOWED_LINEAGE_DEPTH:
        raise ValueError(f"max_depth too high — choose {MAX_ALLOWED_LINEAGE_DEPTH} or less.")
    if depth < 1:
        depth = 1

    schema, table_name = _get_table_schema_and_name(table)
    target_node = _obj_node_id(schema, table_name, column)

    nodes: Dict[str, Dict[str, Any]] = {}
    edges: List[Dict[str, Any]] = []  # {source, target, relation}
    mappings: List[Dict[str, Any]] = []

    # 1) start: procedures that write into target column
    writers = _find_writing_procs(schema, table_name, column)

    nodes[target_node] = {"type": "column", "schema": schema, "name": table_name, "column": column}

    queue: List[Tuple[int, int, str]] = []  # (object_id, depth, via_proc_node)
    for proc in writers:
        proc_node = _obj_node_id(proc["schema"], proc["name"])
        nodes[proc_node] = {"type": "procedure", "schema": proc["schema"], "name": proc["name"]}
        edges.append({"source": proc_node, "target": target_node, "relation": "writes"})
        for e in proc["expressions"]:
            mappings.append({
                "target": {"schema": schema, "table": table_name, "column": column},
                "proc": {"schema": proc["schema"], "name": proc["name"]},
                "expression": e,
            })
        queue.append((proc["object_id"], 1, proc_node))

    # 2) BFS upstream via dependencies
    seen: set = set()
    while queue:
        obj_id, d, via_proc_node = queue.pop(0)
        if d >= depth:
            # we've reached requested depth; don't expand further
            continue
        if obj_id in seen:
            continue
        seen.add(obj_id)

        deps = _get_proc_dependencies(obj_id)
        for dep in deps:
            if not dep["name"]:
                continue
            node_id = _obj_node_id(dep["schema"], dep["name"])
            if node_id not in nodes:
                nodes[node_id] = {"type": dep["type"], "schema": dep["schema"], "name": dep["name"]}
            edges.append({"source": node_id, "target": via_proc_node, "relation": "feeds"})

            # recurse only into procedures (they might in turn read other objects)
            if dep["type"] in ("P",):
                with db_cursor() as cursor:
                    cursor.execute("""
                        SELECT object_id FROM sys.objects
                        WHERE object_id = OBJECT_ID(QUOTENAME(?) + '.' + QUOTENAME(?))
                    """, dep["schema"], dep["name"])
                    row = cursor.fetchone()
                    if row:
                        queue.append((row.object_id, d + 1, node_id))

    return {
        "success": True,
        "effective_max_depth": depth,
        "target": {"schema": schema, "table": table_name, "column": column},
        "graph": {"nodes": nodes, "edges": edges},
        "candidate_population_expressions": mappings,
        "notes": [
            "Expressions parsed best-effort for INSERT/UPDATE/MERGE; dynamic SQL may not be detected.",
            "Dependencies from sys.sql_expression_dependencies require VIEW DEFINITION permission.",
            f"Depth limited by MAX_ALLOWED_LINEAGE_DEPTH={MAX_ALLOWED_LINEAGE_DEPTH}.",
        ],
    }

@mcp.tool
def ask_column_lineage(prompt: str, max_depth: Optional[int] = None) -> Dict[str, object]:
    """
    Free-text wrapper around get_column_lineage.
    Accepts prompts like:
      - "how is column salary populated in table employees"
      - "how is Salary populated"
    If table cannot be inferred, asks for clarification.
    """
    # Determine effective depth using same rules
    depth = DEFAULT_LINEAGE_MAX_DEPTH if (max_depth is None) else int(max_depth)
    if depth > MAX_ALLOWED_LINEAGE_DEPTH:
        raise ValueError(f"max_depth too high — choose {MAX_ALLOWED_LINEAGE_DEPTH} or less.")
    if depth < 1:
        depth = 1

    # Heuristics to extract column & table
    # Pattern: "column {col} in table {table}"
    m = re.search(r"column\s+([A-Za-z0-9_]+)\s+in\s+table\s+([A-Za-z0-9_\.]+)", prompt, re.I)
    if m:
        col, table = m.group(1), m.group(2)
        return get_column_lineage(table=table, column=col, max_depth=depth)

    # Pattern: "how is {col} populated"
    m = re.search(r"how\s+is\s+([A-Za-z0-9_]+)\s+populated", prompt, re.I)
    if m:
        col = m.group(1)
        # If table is ambiguous, try to find any table containing that column name
        # If multiple matches, ask for clarification.
        with _schema_lock:
            candidates = []
            for tkey, tname in db_schema_cache["tables"].items():
                cols = db_schema_cache["columns"].get(tkey, {})
                if col.lower() in cols:
                    candidates.append(tname)
        if len(candidates) == 1:
            return get_column_lineage(table=candidates[0], column=col, max_depth=depth)
        elif len(candidates) > 1:
            return {
                "success": False,
                "message": f"Column '{col}' exists in multiple tables. Please specify one of: {sorted(candidates)}."
            }
        else:
            return {"success": False, "message": f"Could not find column '{col}' in known tables. Specify the table name."}

    return {"success": False, "message": "Could not parse table/column from prompt. Try 'how is column <col> populated in table <schema.table>'."}

# ---- Resources ----
@mcp.resource(
    uri="sql://index",
    description="Overview of available SQL metadata: counts and quick links to tables/jobs.",
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
    description="Markdown list of all base tables discovered in the target database.",
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

# -----------------------
# Entry Point
# -----------------------
if __name__ == "__main__":
    _startup()

    use_http = os.getenv("MCP_HTTP", "1") == "1"
    host = os.getenv("MCP_HOST", "127.0.0.1")
    port = int(os.getenv("MCP_PORT", "8000"))

    if use_http:
        logger.info(f"Starting FastMCP HTTP server on http://{host}:{port}/mcp")
        mcp.run(transport="http", host=host, port=port)
    else:
        logger.info("Starting FastMCP server in STDIO mode")
        mcp.run()
