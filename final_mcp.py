import os
import logging
from typing import Literal, Dict
from contextlib import contextmanager, asynccontextmanager
from dotenv import load_dotenv

import pyodbc
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# --- Load environment variables ---
load_dotenv()

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- FastAPI App ---
app = FastAPI(
    title="SQL MCP Tool",
    description="MCP-compliant tool to query SQL Server metadata.",
    version="1.1.0"
)

# --- Database Config ---
DB_CONFIG = {
    'server': os.getenv('DB_SERVER'),
    'database': os.getenv('DB_NAME'),
    'username': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASS'),
    'driver': '{ODBC Driver 17 for SQL Server}',
}

# --- Schema Cache ---
db_schema_cache = {
    "tables": {},
    "columns": {},
    "objects": {},
    "jobs": {},
}

# --- Database Connection and Cursor Management ---
def get_db_connection():
    conn_str = (
        f"DRIVER={DB_CONFIG['driver']};"
        f"SERVER={DB_CONFIG['server']};"
        f"DATABASE={DB_CONFIG['database']};"
        f"UID={DB_CONFIG['username']};"
        f"PWD={DB_CONFIG['password']}"
    )
    try:
        return pyodbc.connect(conn_str)
    except Exception as e:
        logger.error(f"DB connection failed: {e}")
        raise HTTPException(status_code=500, detail="Database connection failed.")

@contextmanager
def db_cursor():
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        yield cursor
    finally:
        conn.close()

# --- Schema Loading ---
def load_schema_cache():
    logger.info("Loading schema cache...")
    with db_cursor() as cursor:
        cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
        tables = [row.TABLE_NAME for row in cursor.fetchall()]
        db_schema_cache["tables"] = {t.lower(): t for t in tables}

        for table in tables:
            cursor.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ?", table)
            columns = [row.COLUMN_NAME for row in cursor.fetchall()]
            db_schema_cache["columns"][table.lower()] = {c.lower(): c for c in columns}

        cursor.execute("SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES UNION SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS")
        db_schema_cache["objects"] = {row.ROUTINE_NAME.lower(): row.ROUTINE_NAME for row in cursor.fetchall()}

        cursor.execute("SELECT name FROM msdb.dbo.sysjobs")
        db_schema_cache["jobs"] = {row.name.lower(): row.name for row in cursor.fetchall()}

    logger.info("Schema cache loaded.")

@asynccontextmanager
async def lifespan(app: FastAPI):
    load_schema_cache()
    yield

app.router.lifespan_context = lifespan

# --- Input Schema ---
class ToolInput(BaseModel):
    tool: Literal[
        "get_column_data",
        "get_column_population_logic",
        "get_table_schema",
        "get_object_definition",
        "get_job_status"
    ]
    parameters: Dict

# --- Handlers ---
def validate_table_column(table: str, column: str = None):
    table_key = table.lower()
    real_table = db_schema_cache["tables"].get(table_key)
    if not real_table:
        raise HTTPException(status_code=404, detail=f"Table '{table}' not found.")
    if column:
        real_column = db_schema_cache["columns"].get(table_key, {}).get(column.lower())
        if not real_column:
            raise HTTPException(status_code=404, detail=f"Column '{column}' not found in table '{table}'.")
        return real_table, real_column
    return real_table

def handle_get_column_data(params: dict):
    table = params.get("table")
    select_col = params.get("select_col")
    where_col = params.get("where_col")
    value = params.get("value")

    if not all([table, select_col, where_col, value]):
        raise HTTPException(status_code=400, detail="Missing required parameters.")

    table_name = validate_table_column(table)[0]
    select_col_real = validate_table_column(table, select_col)[1]
    where_col_real = validate_table_column(table, where_col)[1]

    query = f"SELECT TOP 20 {select_col_real} FROM {table_name} WHERE {where_col_real} = ?"

    with db_cursor() as cursor:
        try:
            cursor.execute(query, value)
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise HTTPException(status_code=500, detail="Query execution failed.")
        rows = cursor.fetchall()
        results = [row[0] for row in rows]
    return {"success": True, "results": results}

def handle_get_table_schema(params: dict):
    table = params.get("table")
    table_name = validate_table_column(table)[0]

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
        query = "SELECT ROUTINE_NAME, ROUTINE_DEFINITION FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE = 'PROCEDURE'"
        cursor.execute(query)
        procedures = cursor.fetchall()
        matches = []
        for proc in procedures:
            if proc.ROUTINE_DEFINITION and column.lower() in proc.ROUTINE_DEFINITION.lower():
                if "insert into" in proc.ROUTINE_DEFINITION.lower() or "update" in proc.ROUTINE_DEFINITION.lower():
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
        if row and row.definition:
            return {"success": True, "object": real_object, "definition": row.definition}
        else:
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
                "last_run_time": row.run_time
            }
        else:
            raise HTTPException(status_code=404, detail="No run history found.")

# --- Tool Use Endpoint ---
@app.post("/v1/tool-use")
def tool_use(input: ToolInput):
    tool = input.tool
    params = input.parameters
    try:
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
            raise HTTPException(status_code=400, detail="Unknown tool.")
    except HTTPException as e:
        logger.error(f"Error in tool '{tool}': {e.detail}")
        raise e

# --- Metadata Endpoint ---
@app.get("/v1/metadata")
def metadata():
    return {
        "name": "sql_mcp_tool",
        "description": "Tool for querying SQL Server metadata using structured parameters.",
        "tools": [
            {
                "tool": "get_column_data",
                "description": "Fetch data from a specific column with a WHERE condition.",
                "parameters": {
                    "table": "Name of the table",
                    "select_col": "Column to select",
                    "where_col": "Column to filter on",
                    "value": "Value for the filter"
                }
            },
            {
                "tool": "get_column_population_logic",
                "description": "Retrieve procedures that populate a specific column.",
                "parameters": {
                    "column": "Name of the column"
                }
            },
            {
                "tool": "get_table_schema",
                "description": "Get schema of a specified table.",
                "parameters": {
                    "table": "Name of the table"
                }
            },
            {
                "tool": "get_object_definition",
                "description": "Get definition of a database object (procedure/view).",
                "parameters": {
                    "object": "Name of the object"
                }
            },
            {
                "tool": "get_job_status",
                "description": "Get the last run status of a SQL Agent job.",
                "parameters": {
                    "job": "Name of the job"
                }
            }
        ]
    }

# --- Endpoint to refresh schema cache ---
@app.post("/v1/refresh-schema")
def refresh_schema():
    load_schema_cache()
    return {"success": True, "message": "Schema cache refreshed."}
