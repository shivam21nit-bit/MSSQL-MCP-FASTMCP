import os
import logging
from typing import Dict, Optional
from contextlib import contextmanager, asynccontextmanager

from dotenv import load_dotenv
import pyodbc
from fastapi import FastAPI, HTTPException, APIRouter
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

# -----------------------
# Env & Logging
# -----------------------
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("sql-mcp")

# The exact origin of your UI (SCHEME + HOST [+ :PORT]), e.g. https://mygenaidev.o9solutions.com
UI_ORIGIN = os.getenv("UI_ORIGIN", "https://mygenaidev.o9solutions.com")

# -----------------------
# FastAPI App
# -----------------------
app = FastAPI(
    title="SQL MCP Tool",
    description="MCP-compliant tool to query SQL Server metadata.",
    version="1.3.0",
)

# CORS: match the UI origin (credentials allowed). If you don't need cookies, you may set allow_credentials=False and allow_origins=["*"].
app.add_middleware(
    CORSMiddleware,
    allow_origins=[UI_ORIGIN],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization"],
    expose_headers=["Content-Type"],
)

# -----------------------
# DB Config
# -----------------------
DB_CONFIG = {
    "server": os.getenv("DB_SERVER"),
    "database": os.getenv("DB_NAME"),
    "username": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASS"),
    "driver": "{ODBC Driver 17 for SQL Server}",
    "timeout": int(os.getenv("DB_TIMEOUT", "30")),
    "login_timeout": int(os.getenv("DB_LOGIN_TIMEOUT", "15")),
}

def _validate_db_env():
    missing = [k for k in ("server", "database", "username", "password") if not DB_CONFIG[k]]
    if missing:
        logger.warning("Missing DB env keys: %s", ", ".join(missing))
_validate_db_env()

# -----------------------
# In-memory Schema Cache
# -----------------------
db_schema_cache = {
    "tables": {},   # {lower: real}
    "columns": {},  # {table_lower: {col_lower: real}}
    "objects": {},  # {lower: real}
    "jobs": {},     # {lower: real}
}

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
    try:
        conn = pyodbc.connect(conn_str)
        try:
            conn.autocommit = True
        except Exception:
            pass
        return conn
    except Exception as e:
        logger.error("DB connection failed: %s", e)
        raise HTTPException(status_code=500, detail="Database connection failed.")

from contextlib import contextmanager
@contextmanager
def db_cursor():
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        yield cursor
    finally:
        try:
            conn.close()
        except Exception:
            pass

# -----------------------
# Cache Loader
# -----------------------
def load_schema_cache():
    logger.info("Loading schema cache...")
    with db_cursor() as cursor:
        # Tables
        cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
        tables = [row.TABLE_NAME for row in cursor.fetchall()]
        db_schema_cache["tables"] = {t.lower(): t for t in tables}

        # Columns per table
        for table in tables:
            cursor.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ?", table)
            cols = [row.COLUMN_NAME for row in cursor.fetchall()]
            db_schema_cache["columns"][table.lower()] = {c.lower(): c for c in cols}

        # Stored procs + views
        cursor.execute(
            "SELECT ROUTINE_NAME AS obj FROM INFORMATION_SCHEMA.ROUTINES "
            "UNION SELECT TABLE_NAME AS obj FROM INFORMATION_SCHEMA.VIEWS"
        )
        db_schema_cache["objects"] = {row.obj.lower(): row.obj for row in cursor.fetchall()}

        # SQL Agent jobs (best-effort)
        try:
            cursor.execute("SELECT name FROM msdb.dbo.sysjobs")
            db_schema_cache["jobs"] = {row.name.lower(): row.name for row in cursor.fetchall()}
        except Exception as e:
            logger.warning("Unable to load SQL Agent jobs: %s", e)

    logger.info("Schema cache loaded: %d tables, %d objects, %d jobs",
                len(db_schema_cache["tables"]),
                len(db_schema_cache["objects"]),
                len(db_schema_cache["jobs"]))

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        load_schema_cache()
    except Exception as e:
        logger.exception("Schema cache load failed at startup; continuing. Error: %s", e)
    yield

app.router.lifespan_context = lifespan

# -----------------------
# Request Model (accepts both shapes)
# -----------------------
class ToolInput(BaseModel):
    tool: Optional[str] = Field(default=None, description="Tool name (legacy shape).")
    name: Optional[str] = Field(default=None, description="Tool name (common MCP shape).")
    parameters: Optional[Dict] = Field(default=None, description="Tool args (legacy).")
    arguments: Optional[Dict] = Field(default=None, description="Tool args (common MCP).")

# -----------------------
# Validators
# -----------------------
def validate_table_column(table: str, column: str = None):
    if not table:
        raise HTTPException(status_code=400, detail="Missing table name.")
    table_key = table.lower()
    real_table = db_schema_cache["tables"].get(table_key)
    if not real_table:
        raise HTTPException(status_code=404, detail=f"Table '{table}' not found.")
    if column:
        real_column = db_schema_cache["columns"].get(table_key, {}).get(column.lower())
        if not real_column:
            raise HTTPException(status_code=404, detail=f"Column '{column}' not found in table '{table}'.")
        return real_table, real_column
    return real_table, None

# -----------------------
# Tool Handlers
# -----------------------
def handle_get_column_data(params: dict):
    table = params.get("table")
    select_col = params.get("select_col")
    where_col = params.get("where_col")
    value = params.get("value")

    if not all([table, select_col, where_col, value]):
        raise HTTPException(status_code=400, detail="Missing required parameters.")

    table_name, _ = validate_table_column(table)
    _, select_col_real = validate_table_column(table, select_col)
    _, where_col_real = validate_table_column(table, where_col)

    query = f"SELECT TOP 20 {select_col_real} FROM {table_name} WHERE {where_col_real} = ?"
    with db_cursor() as cursor:
        try:
            cursor.execute(query, value)
            rows = cursor.fetchall()
        except Exception as e:
            logger.error("Query execution failed: %s", e)
            raise HTTPException(status_code=500, detail="Query execution failed.")
    results = [row[0] for row in rows]
    return {"success": True, "results": results}

def handle_get_table_schema(params: dict):
    table = params.get("table")
    table_name, _ = validate_table_column(table)

    query = "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ?"
    with db_cursor() as cursor:
        cursor.execute(query, table_name)
        rows = cursor.fetchall()
        columns = [dict(zip([desc[0] for desc in cursor.description], row)) for row in rows]
    return {"success": True, "table": table_name, "columns": columns}

def handle_get_column_population_logic(params: dict):
    column = params.get("column")
    if not column:
        raise HTTPException(status_code=400, detail="Missing column name.")

    with db_cursor() as cursor:
        query = ("SELECT ROUTINE_NAME, ROUTINE_DEFINITION "
                 "FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE = 'PROCEDURE'")
        cursor.execute(query)
        procedures = cursor.fetchall()
        matches = []
        for proc in procedures:
            definition = getattr(proc, "ROUTINE_DEFINITION", None)
            if not definition:
                continue
            low = definition.lower()
            if column.lower() in low and ("insert into" in low or "update" in low):
                matches.append(proc.ROUTINE_NAME)
    return {"success": True, "column": column, "procedures": matches}

def handle_get_object_definition(params: dict):
    object_name = params.get("object")
    if not object_name:
        raise HTTPException(status_code=400, detail="Missing object name.")

    real_object = db_schema_cache["objects"].get(object_name.lower())
    if not real_object:
        raise HTTPException(status_code=404, detail="Object not found.")

    query = "SELECT OBJECT_DEFINITION(OBJECT_ID(?)) AS definition"
    with db_cursor() as cursor:
        cursor.execute(query, real_object)
        row = cursor.fetchone()
        definition = row.definition if row else None
        if definition:
            return {"success": True, "object": real_object, "definition": definition}
        raise HTTPException(status_code=404, detail="Definition not found.")

def handle_get_job_status(params: dict):
    job_name = params.get("job")
    if not job_name:
        raise HTTPException(status_code=400, detail="Missing job name.")

    real_job = db_schema_cache["jobs"].get(job_name.lower())
    if not real_job:
        raise HTTPException(status_code=404, detail="Job not found.")

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
        if row:
            return {
                "success": True,
                "job": row.name,
                "status": row.status,
                "last_run_date": row.run_date,
                "last_run_time": row.run_time,
            }
        raise HTTPException(status_code=404, detail="No run history found.")

# -----------------------
# Tool-use Endpoints
# -----------------------
class _HTTPError(HTTPException):
    pass

def _dispatch_tool(tool: str, params: Dict):
    if tool == "get_column_data":
        return handle_get_column_data(params)
    elif tool == "get_column_population_logic":
        return handle_get_column_population_logic(params)
    elif tool == "get_table_schema":
        return handle_get_table_schema(params)
    elif tool == "get_object_definition":
        return handle_get_object_definition(params)
    elif tool == "get_job_status":
        return handle_get_job_status(params)
    else:
        raise HTTPException(status_code=400, detail=f"Unknown tool '{tool}'.")

@app.post("/v1/tool-use")
def tool_use(input: ToolInput):
    tool = input.tool or input.name
    params = input.parameters or input.arguments or {}
    if not tool:
        raise HTTPException(status_code=400, detail="Missing tool name.")
    try:
        return _dispatch_tool(tool, params)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Unhandled error in tool '%s': %s", tool, e)
        raise HTTPException(status_code=500, detail="Internal server error.")

# Aliases that some UIs expect
@app.post("/mcp/tool-use")
@app.post("/tool-use")
def tool_use_alias(input: ToolInput):
    return tool_use(input)

# -----------------------
# Health / Root
# -----------------------
@app.get("/")
def root():
    return {"ok": True, "service": "sql_mcp_tool"}

@app.get("/health")
def health():
    return {"status": "healthy"}

# -----------------------
# Discovery Endpoints
# -----------------------
discovery = APIRouter()

def json_schema_for(props: Dict[str, str], required: list[str]):
    return {
        "type": "object",
        "properties": {k: {"type": "string", "description": v} for k, v in props.items()},
        "required": required,
        "additionalProperties": False,
    }

@discovery.get("/v1/metadata")
@discovery.get("/mcp/tools")
@discovery.get("/tools")
def mcp_metadata():
    return {
        "name": "sql_mcp_tool",
        "description": "Tool for querying SQL Server metadata using structured parameters.",
        "tools": [
            {
                "name": "get_column_data",
                "description": "Fetch data from a specific column with a WHERE condition.",
                "input_schema": json_schema_for(
                    {
                        "table": "Name of the table",
                        "select_col": "Column to select",
                        "where_col": "Column to filter on",
                        "value": "Value for the filter",
                    },
                    required=["table", "select_col", "where_col", "value"],
                ),
            },
            {
                "name": "get_column_population_logic",
                "description": "Retrieve procedures that populate a specific column.",
                "input_schema": json_schema_for(
                    {"column": "Name of the column"},
                    required=["column"],
                ),
            },
            {
                "name": "get_table_schema",
                "description": "Get schema of a specified table.",
                "input_schema": json_schema_for(
                    {"table": "Name of the table"},
                    required=["table"],
                ),
            },
            {
                "name": "get_object_definition",
                "description": "Get definition of a database object (procedure/view).",
                "input_schema": json_schema_for(
                    {"object": "Name of the object"},
                    required=["object"],
                ),
            },
            {
                "name": "get_job_status",
                "description": "Get the last run status of a SQL Agent job.",
                "input_schema": json_schema_for(
                    {"job": "Name of the job"},
                    required=["job"],
                ),
            },
        ],
    }

# Some UIs probe /mcp and expect JSON (avoid redirects that drop CORS)
@discovery.get("/mcp")
def mcp_base():
    return mcp_metadata()

# Optional: helps some toolchains auto-discover your service
@discovery.get("/.well-known/ai-plugin.json")
def well_known():
    return {
        "name_for_human": "SQL MCP Tool",
        "name_for_model": "sql_mcp_tool",
        "schema_version": "v1",
        "api": {"type": "openapi", "url": "/openapi.json"},
        "auth": {"type": "none"},
        "description_for_model": "Query SQL Server metadata (schemas, objects, jobs).",
        "description_for_human": "SQL Server metadata tools.",
    }

app.include_router(discovery)

# -----------------------
# Admin: Refresh Cache
# -----------------------
@app.post("/v1/refresh-schema")
def refresh_schema():
    try:
        load_schema_cache()
        return {"success": True, "message": "Schema cache refreshed."}
    except Exception as e:
        logger.exception("Refresh schema failed: %s", e)
        raise HTTPException(status_code=500, detail="Failed to refresh schema.")
