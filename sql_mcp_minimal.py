"""
SQL MCP Tool (FastMCP 2.0)
--------------------------
Minimal FastMCP-based MCP server exposing SQL Server metadata utilities.

HTTP: set MCP_HTTP=1 to run with Uvicorn at http://<host>:<port>/mcp
STDIO: default (no env needed), good for Claude Desktop.

Requires:
  pip install fastmcp python-dotenv pyodbc uvicorn
"""

import os
import logging
import threading
from typing import Dict, Optional, Tuple
from contextlib import contextmanager

from dotenv import load_dotenv
import pyodbc
from fastmcp import FastMCP
from fastmcp.transport.http import make_asgi_app  # ASGI wrapper for HTTP

# -----------------------
# Env & Logging
# -----------------------
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("sql-mcp")

DB_CONFIG: Dict[str, object] = {
    "server": os.getenv("DB_SERVER"),
    "database": os.getenv("DB_NAME"),
    "username": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASS"),
    "driver": os.getenv("DB_DRIVER", "{ODBC Driver 17 for SQL Server}"),
    "timeout": int(os.getenv("DB_TIMEOUT", "30")),
    "login_timeout": int(os.getenv("DB_LOGIN_TIMEOUT", "15")),
}

# -----------------------
# In-memory Schema Cache (thread-safe)
# -----------------------
_schema_lock = threading.RLock()
db_schema_cache = {"tables": {}, "columns": {}, "objects": {}, "jobs": {}}

# -----------------------
# DB Helpers
# -----------------------
def get_db_connection():
    conn_str = (
        f"DRIVER={DB_CONFIG['driver']};"
        f"SERVER={DB_CONFIG['server']};"
        f"DATABASE={DB_CONFIG['database']};"
        f"UID={DB_CONFIG['username']};"
        f"PWD={DB_CONFIG['password']};"
        f"Timeout={DB_CONFIG['timeout']};"
        f"LoginTimeout={DB_CONFIG['login_timeout']}"
    )
    return pyodbc.connect(conn_str, autocommit=True)

@contextmanager
def db_cursor():
    conn = get_db_connection()
    try:
        yield conn.cursor()
    finally:
        conn.close()

# -----------------------
# Cache Loader
# -----------------------
def load_schema_cache() -> Dict[str, int]:
    with _schema_lock:
        with db_cursor() as cursor:
            cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
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
# MCP Server, Tools & Resources
# -----------------------
mcp = FastMCP("SQL MCP Tool")

def _startup():
    try:
        load_schema_cache()
    except Exception as e:
        logger.warning("Schema cache load failed: %s", e)

# ---- Tools ----
@mcp.tool
def refresh_schema() -> Dict[str, object]:
    return {"success": True, **load_schema_cache()}

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
            definition = getattr(proc, "ROUTINE_DEFINITION", "")
            if column.lower() in definition.lower() and ("insert into" in definition.lower() or "update" in definition.lower()):
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
    return {"success": True, "job": row.name, "status": row.status, "last_run_date": row.run_date, "last_run_time": row.run_time} if row else {"success": False}

# ---- Resources ----
@mcp.resource(
    uri="sql://column-population/{column}",
    description="Markdown report of how a column is populated across stored procedures.",
    mime_type="text/markdown",
    annotations={"readOnlyHint": True, "idempotentHint": True}
)
def resource_column_population(column: str) -> str:
    with db_cursor() as cursor:
        cursor.execute("SELECT ROUTINE_NAME, ROUTINE_DEFINITION FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE='PROCEDURE'")
        matches = []
        for proc in cursor.fetchall():
            definition = getattr(proc, "ROUTINE_DEFINITION", "")
            if column.lower() in definition.lower() and ("insert into" in definition.lower() or "update" in definition.lower()):
                matches.append((proc.ROUTINE_NAME, definition))
    if not matches:
        return f"# Column Population Report\n\nNo procedures found that populate `{column}`."
    lines = [f"# Column Population Report: `{column}`\n"]
    for name, definition in matches:
        lines.append(f"## {name}\n```sql\n{definition[:400]}...\n```")
    return "\n".join(lines)

@mcp.resource(
    uri="sql://job-status/{job}",
    description="Markdown summary of a SQL Agent job's last status and most recent failure detail.",
    mime_type="text/markdown",
    annotations={"readOnlyHint": True, "idempotentHint": True}
)
def resource_job_status(job: str) -> str:
    with _schema_lock:
        real_job = db_schema_cache["jobs"].get(job.lower())
    if not real_job:
        return f"# Job Status\n\nJob `{job}` not found."
    with db_cursor() as cursor:
        cursor.execute(
            "SELECT TOP 1 j.name, h.run_date, h.run_time, "
            "CASE h.run_status WHEN 0 THEN 'Failed' WHEN 1 THEN 'Succeeded' "
            "WHEN 2 THEN 'Retry' WHEN 3 THEN 'Canceled' ELSE 'Running' END AS status "
            "FROM msdb.dbo.sysjobs j "
            "LEFT JOIN msdb.dbo.sysjobhistory h ON j.job_id = h.job_id "
            "WHERE j.name = ? AND h.step_id = 0 "
            "ORDER BY h.run_date DESC, h.run_time DESC",
            real_job,
        )
        outcome = cursor.fetchone()
        cursor.execute(
            "SELECT TOP 1 h.run_date, h.run_time, h.step_id, h.step_name, h.message "
            "FROM msdb.dbo.sysjobhistory h "
            "JOIN msdb.dbo.sysjobs j ON j.job_id = h.job_id "
            "WHERE j.name = ? AND h.run_status = 0 AND h.step_id > 0 "
            "ORDER BY h.run_date DESC, h.run_time DESC",
            real_job,
        )
        failure = cursor.fetchone()
    lines = [f"# Job Status: `{real_job}`\n"]
    if outcome:
        lines.append(f"**Last Outcome:** {outcome.status} on {outcome.run_date} {outcome.run_time}")
    if failure:
        lines.append(f"\n**Most Recent Failure (Step {failure.step_id} - {failure.step_name}):**\n")
        lines.append("```\n" + str(failure.message) + "\n```")
    return "\n".join(lines)

@mcp.resource(
    uri="sql://index",
    description="Overview of available SQL metadata: counts and quick links to tables/jobs.",
    mime_type="text/markdown",
    annotations={"readOnlyHint": True, "idempotentHint": True}
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
    annotations={"readOnlyHint": True, "idempotentHint": True}
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
    annotations={"readOnlyHint": True, "idempotentHint": True}
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
# Build ASGI app for HTTP
# -----------------------
asgi_app = make_asgi_app(mcp, path="/mcp")

# -----------------------
# Entry Point
# -----------------------
if __name__ == "__main__":
    # Preload cache so discovery works immediately
    try:
        _startup()
    except Exception as e:
        logger.warning("Startup cache load failed: %s", e)

    use_http = os.getenv("MCP_HTTP", "0") == "1"
    if use_http:
        import uvicorn
        host = os.getenv("MCP_HOST", "127.0.0.1")
        port = int(os.getenv("MCP_PORT", "8000"))
        logger.info(f"Starting HTTP MCP server on http://{host}:{port}/mcp")
        uvicorn.run(asgi_app, host=host, port=port)
    else:
        # STDIO mode
        mcp.run()
