# app.py (FastAPI Version)
# This script creates a FastAPI web server that acts as a secure API for your agent
# to interact with a SQL Server database in read-only mode.

import spacy
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import pyodbc
import logging
import uvicorn
import re
from contextlib import asynccontextmanager

# --- Basic Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Database Configuration ---
DB_CONFIG = {
    'server': '8RBQW14-PC',
    'database': 'TestDatabase',
    'username': 'readonly_agent',
    'password': 'Phenom@21',
    'driver': '{ODBC Driver 17 for SQL Server}',
}

# --- Schema Cache ---
db_schema_cache = {
    "tables": {},
    "objects": {},
    "jobs": {},
    "columns": {}
}

def get_db_connection():
    """Establishes and returns a connection to the database."""
    conn_str = (
        f"DRIVER={DB_CONFIG['driver']};"
        f"SERVER={DB_CONFIG['server']};"
        f"DATABASE={DB_CONFIG['database']};"
        f"UID={DB_CONFIG['username']};"
        f"PWD={DB_CONFIG['password']}"
    )
    try:
        conn = pyodbc.connect(conn_str)
        return conn
    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        logging.error(f"Database connection failed: {sqlstate}")
        logging.error(ex)
        return None

def load_database_schema_cache():
    """
    Connects to the DB to cache lists of tables, objects,
    and jobs for faster and more reliable matching.
    """
    logging.info("Initializing database schema cache...")
    conn = get_db_connection()
    if not conn:
        logging.error("Could not connect to database to build schema cache. Server may not function correctly.")
        return

    try:
        with conn.cursor() as cursor:
            # Cache tables and columns
            cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
            tables = [row.TABLE_NAME for row in cursor.fetchall()]
            db_schema_cache["tables"] = {name.lower(): name for name in tables}
            for table_name in tables:
                cursor.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ?", table_name)
                columns = [row.COLUMN_NAME for row in cursor.fetchall()]
                db_schema_cache["columns"][table_name.lower()] = {name.lower(): name for name in columns}
            # Cache objects
            cursor.execute("SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES UNION SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS")
            db_schema_cache["objects"] = {row.ROUTINE_NAME.lower(): row.ROUTINE_NAME for row in cursor.fetchall()}
            # Cache jobs
            cursor.execute("SELECT name FROM msdb.dbo.sysjobs")
            db_schema_cache["jobs"] = {row.name.lower(): row.name for row in cursor.fetchall()}
            logging.info(f"Cache loaded. Found {len(db_schema_cache['tables'])} tables, {len(db_schema_cache['objects'])} objects, {len(db_schema_cache['jobs'])} jobs.")
    except pyodbc.Error as ex:
        logging.error(f"Error building schema cache: {ex}")
    finally:
        if conn:
            conn.close()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # This code runs once on server startup
    print("Server is starting up...")
    load_database_schema_cache()
    yield
    # Code below yield would run on shutdown
    print("Server is shutting down...")

# --- FastAPI and NLP Initialization ---
app = FastAPI(
    title="SQL Server MCP",
    description="An API to analyze a SQL Server database using natural language queries.",
    version="2.5.1", # Version updated for syntax cleanup
    lifespan=lifespan
)

# --- Global Exception Handler ---
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logging.error(f"An unhandled exception occurred for request {request.url}: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"error": "An unexpected internal server error occurred.", "detail": str(exc)},
    )

try:
    nlp = spacy.load("en_core_web_sm")
    logging.info("spaCy NLP model 'en_core_web_sm' loaded successfully.")
except OSError:
    logging.error("spaCy model 'en_core_web_sm' not found.")
    logging.info("Please run 'python -m spacy download en_core_web_sm' in your terminal.")
    exit()

class Query(BaseModel):
    query: str

@app.post("/analyze")
def analyze_database(item: Query):
    user_query = item.query
    if not user_query:
        raise HTTPException(status_code=400, detail="Query cannot be empty.")
    
    logging.info(f"Received query: '{user_query}'")
    query_lower = user_query.lower()
    
    if "how is" in query_lower and "populated" in query_lower:
        return get_column_population_logic(query_lower)
    elif "data of" in query_lower or "value of" in query_lower:
        return get_column_data(query_lower)
    elif "job status" in query_lower or "status of job" in query_lower:
        return get_job_status(query_lower)
    elif "definition of" in query_lower or "logic of" in query_lower or "code for" in query_lower:
        return get_object_definition(query_lower)
    else:
        return get_table_schema(query_lower)

def get_column_population_logic(query_lower: str):
    match = re.search(r"column\s+([a-zA-Z0-9_]+)", query_lower)
    if not match:
        raise HTTPException(status_code=400, detail="Please specify the column name, for example: 'how is column EmployeeID populated'.")
    
    column_name = match.group(1)
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection could not be established.")
    try:
        with conn.cursor() as cursor:
            query = "SELECT ROUTINE_NAME, ROUTINE_DEFINITION FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE = 'PROCEDURE'"
            cursor.execute(query)
            procedures = cursor.fetchall()
            modifying_procs = []
            search_pattern = re.compile(f'\\b{column_name}\\b', re.IGNORECASE)
            for proc in procedures:
                if proc.ROUTINE_DEFINITION and search_pattern.search(proc.ROUTINE_DEFINITION):
                    if 'insert into' in proc.ROUTINE_DEFINITION.lower() or 'update' in proc.ROUTINE_DEFINITION.lower():
                        modifying_procs.append(proc.ROUTINE_NAME)
            if not modifying_procs:
                 return {"column_name": column_name, "message": "No stored procedures found that appear to populate this column."}
            return {"column_name": column_name, "populated_by_procedures": modifying_procs}
    except pyodbc.Error as ex:
        logging.error(f"Error in get_column_population_logic: {ex}")
        raise HTTPException(status_code=500, detail="An error occurred while analyzing stored procedures.")

def get_column_data(query_lower: str):
    pattern = r"data of\s+(?P<select_col>\w+)\s+from\s+(?P<table>\w+)\s+where\s+(?P<where_col>\w+)\s+is\s+(?P<value>\w+)"
    match = re.search(pattern, query_lower)
    if not match:
        raise HTTPException(status_code=400, detail="Query for data must be structured as: 'data of [column] from [table] where [column] is [value]'.")
    
    parts = match.groupdict()
    
    table_name_lower = parts["table"].lower()
    table_name = db_schema_cache["tables"].get(table_name_lower)

    if not table_name:
        raise HTTPException(status_code=404, detail=f"Table '{parts['table']}' not found in cache.")

    select_col = db_schema_cache["columns"].get(table_name_lower, {}).get(parts["select_col"].lower())
    where_col = db_schema_cache["columns"].get(table_name_lower, {}).get(parts["where_col"].lower())
    value = parts["value"]

    if not all([select_col, where_col]):
        raise HTTPException(status_code=404, detail="Could not find the specified columns in the database cache.")
    
    query = f"SELECT TOP 20 {select_col} FROM {table_name} WHERE {where_col} = ?"
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection could not be established.")
    try:
        with conn.cursor() as cursor:
            try:
                numeric_value = int(value)
                cursor.execute(query, numeric_value)
            except ValueError:
                cursor.execute(query, value)
            rows = cursor.fetchall()
            results = [row[0] for row in rows]
            return {"query": match.group(0), "results": results}
    except pyodbc.Error as ex:
        logging.error(f"Error executing get_column_data query: {ex}")
        raise HTTPException(status_code=500, detail="An error occurred while fetching data.")

def get_table_schema(query_lower: str):
    found_table = next((orig for low, orig in db_schema_cache["tables"].items() if low in query_lower), None)
    if not found_table:
        raise HTTPException(status_code=400, detail="Could not identify a known table name in your query.")
    query = "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ?"
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection could not be established.")
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, found_table)
            rows = cursor.fetchall()
            if not rows:
                raise HTTPException(status_code=404, detail=f"Table '{found_table}' not found or has no columns.")
            return [dict(zip([c[0] for c in cursor.description], row)) for row in rows]
    except pyodbc.Error as ex:
        logging.error(f"Error in get_table_schema: {ex}")
        raise HTTPException(status_code=500, detail="An error occurred while fetching the table schema.")

def get_object_definition(query_lower: str):
    found_object = next((orig for low, orig in db_schema_cache["objects"].items() if low in query_lower), None)
    if not found_object:
        raise HTTPException(status_code=400, detail="Could not identify a procedure or view name in your query.")
    query = "SELECT OBJECT_DEFINITION(OBJECT_ID(?)) AS definition"
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection could not be established.")
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, found_object)
            row = cursor.fetchone()
            if row and row.definition:
                return {"object_name": found_object, "definition": row.definition}
            else:
                raise HTTPException(status_code=404, detail=f"Object '{found_object}' not found or has no definition.")
    except pyodbc.Error as ex:
        logging.error(f"Error in get_object_definition: {ex}")
        raise HTTPException(status_code=500, detail="An error occurred while fetching the object definition.")

def get_job_status(query_lower: str):
    found_job = next((orig for low, orig in db_schema_cache["jobs"].items() if low in query_lower), None)
    if not found_job:
        raise HTTPException(status_code=400, detail="Could not identify a job name in your query.")
    query = """
    SELECT TOP 1 j.name, h.run_date, h.run_time,
            CASE h.run_status
                WHEN 0 THEN 'Failed' WHEN 1 THEN 'Succeeded' WHEN 2 THEN 'Retry'
                WHEN 3 THEN 'Canceled' ELSE 'Running'
            END AS status
    FROM msdb.dbo.sysjobs j
    LEFT JOIN msdb.dbo.sysjobhistory h ON j.job_id = h.job_id
    WHERE j.name = ? AND h.step_id = 0
    ORDER BY h.run_date DESC, h.run_time DESC;
    """
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection could not be established.")
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, found_job)
            row = cursor.fetchone()
            if row:
                return {"job_name": row.name, "status": row.status, "last_run_date": row.run_date, "last_run_time": row.run_time}
            else:
                raise HTTPException(status_code=404, detail=f"Job '{found_job}' not found or has no run history.")
    except pyodbc.Error as ex:
        logging.error(f"Error in get_job_status: {ex}")
        raise HTTPException(status_code=500, detail="An error occurred while fetching job status.")

if __name__ == '__main__':
    # Running without the reloader for maximum stability.
    uvicorn.run(app, host='127.0.0.1', port=5000)

