# app.py
# This script creates a Flask web server that acts as a secure API for your agent
# to interact with a SQL Server database in read-only mode.

import spacy
from flask import Flask, jsonify, request
import pyodbc
import logging

# --- Basic Logging Setup ---
# This helps in debugging issues when the server is running.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Database Configuration ---
# IMPORTANT: Replace these placeholder values with your actual
# SQL Server connection details.
DB_CONFIG = {
    'server': '8RBQW14-PC',                            # Your server name
    'database': 'TestDatabase',                       # The database you want to analyze
    'username': 'readonly_agent',                     # The user you created
    'password': 'Phenom@21',                          # The password you set for the user
    'driver': '{ODBC Driver 17 for SQL Server}',      # This driver is standard for modern SQL Server versions
}

# --- Flask and NLP Initialization ---
app = Flask(__name__)
try:
    nlp = spacy.load("en_core_web_sm")
    logging.info("spaCy NLP model 'en_core_web_sm' loaded successfully.")
except OSError:
    logging.error("spaCy model 'en_core_web_sm' not found.")
    logging.info("Please run 'python -m spacy download en_core_web_sm' in your terminal.")
    exit()

# --- NEW: Schema Cache ---
# This cache stores database object names for reliable matching,
# instead of relying on the general-purpose NLP model.
db_schema_cache = {
    "tables": {},   # {lowercase_name: OriginalCaseName}
    "objects": {},  # For procedures, views, functions
    "jobs": {}
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
        logging.error(ex) # Log the full error for detailed debugging
        return None

def load_database_schema_cache():
    """
    NEW: Connects to the DB on startup to cache lists of tables, objects,
    and jobs for faster and more reliable matching.
    """
    logging.info("Initializing database schema cache...")
    conn = get_db_connection()
    if not conn:
        logging.error("Could not connect to database to build schema cache. Server may not function correctly.")
        return

    try:
        with conn.cursor() as cursor:
            # Cache table names
            cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
            db_schema_cache["tables"] = {row.TABLE_NAME.lower(): row.TABLE_NAME for row in cursor.fetchall()}

            # Cache names of procedures, views, and functions
            cursor.execute("SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES UNION SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS")
            db_schema_cache["objects"] = {row.ROUTINE_NAME.lower(): row.ROUTINE_NAME for row in cursor.fetchall()}

            # Cache SQL Agent job names
            cursor.execute("SELECT name FROM msdb.dbo.sysjobs")
            db_schema_cache["jobs"] = {row.name.lower(): row.name for row in cursor.fetchall()}
            
            logging.info(f"Cache loaded. Found {len(db_schema_cache['tables'])} tables, {len(db_schema_cache['objects'])} objects, {len(db_schema_cache['jobs'])} jobs.")
    except pyodbc.Error as ex:
        logging.error(f"Error building schema cache: {ex}")
    finally:
        if conn:
            conn.close()


@app.route('/analyze', methods=['POST'])
def analyze_database():
    """
    Analyzes a user's natural language query about the database.
    This is the main endpoint for your agent.
    """
    user_query = request.json.get('query', '')
    if not user_query:
        logging.warning("Received empty query.")
        return jsonify({"error": "Query cannot be empty."}), 400
    
    logging.info(f"Received query: '{user_query}'")
    doc = nlp(user_query)

    query_lower = user_query.lower()
    if "job status" in query_lower or "status of job" in query_lower:
        return get_job_status(query_lower)
    elif "definition of" in query_lower or "logic of" in query_lower or "code for" in query_lower:
        return get_object_definition(query_lower)
    else:
        return get_table_schema(query_lower)


def get_table_schema(query_lower):
    """
    FIXED: Retrieves schema by searching for a known table name from the cache
    within the query string.
    """
    found_table = None
    for table_lower, table_original in db_schema_cache["tables"].items():
        if table_lower in query_lower:
            found_table = table_original
            break
            
    if not found_table:
        logging.warning(f"Could not find a known table name in the query: '{query_lower}'")
        return jsonify({"error": "Could not identify a known table name in your query. Please be more specific."}), 400

    logging.info(f"Found table '{found_table}' in query. Fetching schema.")
    query = "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ?"
    
    conn = get_db_connection()
    if not conn:
        return jsonify({"error": "Database connection could not be established."}), 500
        
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, found_table)
            rows = cursor.fetchall()
            if not rows:
                return jsonify({"error": f"Table '{found_table}' not found or has no columns."}), 404
            
            schema = [dict(zip([column[0] for column in cursor.description], row)) for row in rows]
            return jsonify(schema)
    except pyodbc.Error as ex:
        logging.error(f"Error executing get_table_schema query: {ex}")
        return jsonify({"error": "An error occurred while fetching the table schema."}), 500


def get_object_definition(query_lower):
    """
    FIXED: Retrieves definition by searching for a known object name from the cache.
    """
    found_object = None
    for obj_lower, obj_original in db_schema_cache["objects"].items():
        if obj_lower in query_lower:
            found_object = obj_original
            break

    if not found_object:
        logging.warning("Could not find a known object name in the query.")
        return jsonify({"error": "Could not identify a procedure or view name in your query."}), 400

    logging.info(f"Attempting to find definition for object: {found_object}")
    query = "SELECT OBJECT_DEFINITION(OBJECT_ID(?)) AS definition"
    
    conn = get_db_connection()
    if not conn:
        return jsonify({"error": "Database connection could not be established."}), 500

    try:
        with conn.cursor() as cursor:
            cursor.execute(query, found_object)
            row = cursor.fetchone()
            if row and row.definition:
                return jsonify({"object_name": found_object, "definition": row.definition})
            else:
                return jsonify({"error": f"Object '{found_object}' not found or has no definition."}), 404
    except pyodbc.Error as ex:
        logging.error(f"Error executing get_object_definition query: {ex}")
        return jsonify({"error": "An error occurred while fetching the object definition."}), 500


def get_job_status(query_lower):
    """
    FIXED: Retrieves job status by searching for a known job name from the cache.
    """
    found_job = None
    for job_lower, job_original in db_schema_cache["jobs"].items():
        if job_lower in query_lower:
            found_job = job_original
            break

    if not found_job:
        logging.warning("Could not find a known job name in the query.")
        return jsonify({"error": "Could not identify a job name in your query."}), 400

    logging.info(f"Attempting to find status for job: {found_job}")
    query = """
    SELECT TOP 1 j.name, h.run_date, h.run_time,
            CASE h.run_status
                WHEN 0 THEN 'Failed'
                WHEN 1 THEN 'Succeeded'
                WHEN 2 THEN 'Retry'
                WHEN 3 THEN 'Canceled'
                ELSE 'Running'
            END AS status
    FROM msdb.dbo.sysjobs j
    LEFT JOIN msdb.dbo.sysjobhistory h ON j.job_id = h.job_id
    WHERE j.name = ? AND h.step_id = 0
    ORDER BY h.run_date DESC, h.run_time DESC;
    """
    
    conn = get_db_connection()
    if not conn:
        return jsonify({"error": "Database connection could not be established."}), 500

    try:
        with conn.cursor() as cursor:
            cursor.execute(query, found_job)
            row = cursor.fetchone()
            if row:
                return jsonify({"job_name": row.name, "status": row.status, "last_run_date": row.run_date, "last_run_time": row.run_time})
            else:
                return jsonify({"error": f"Job '{found_job}' not found or has no run history."}), 404
    except pyodbc.Error as ex:
        logging.error(f"Error executing get_job_status query: {ex}")
        return jsonify({"error": "An error occurred while fetching the job status. Ensure the 'readonly_agent' has permissions on msdb."}), 500


if __name__ == '__main__':
    # NEW: Load the schema cache on startup.
    load_database_schema_cache()
    # This makes the server accessible from other machines on your network.
    # Use '127.0.0.1' to keep it accessible only from your local machine.
    app.run(host='0.0.0.0', port=5000, debug=True)

