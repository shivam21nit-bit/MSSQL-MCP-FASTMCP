# SQL MCP Tool — FastMCP v2.x Compatible
# Main Code in Server.py
Expose SQL Server metadata and utilities via [FastMCP](https://pypi.org/project/fastmcp/).  
This tool provides quick access to SQL Server schema, jobs, and object definitions for agents or copilots that support the MCP protocol.

---

## ✨ Features

- **Metadata Introspection**
  - List tables, columns, and objects from SQL Server
  - Retrieve table schema and column definitions
- **Data Exploration**
  - Fetch sample values from a given column with filters
- **Lineage & Logic**
  - Discover which stored procedures populate/update a column
- **Object Inspection**
  - Get full T-SQL definition of stored procedures, views, or functions
- **SQL Agent Integration**
  - Check job status and execution history
- **Resources**
  - Markdown lists of tables and jobs (`sql://tables`, `sql://jobs`)

---

## 🛠 Requirements

```bash
pip install fastmcp python-dotenv pyodbc
# optional: for tunneling
pip install cloudflared
SQL Server ODBC Driver: Ensure ODBC Driver 17 for SQL Server (or newer) is installed on your system.

⚙️ Configuration
Set up environment variables in a .env file:

ini
Copy code
DB_SERVER=localhost
DB_NAME=MyDatabase
DB_USER=sa
DB_PASS=secret
DB_DRIVER={ODBC Driver 17 for SQL Server}
DB_TIMEOUT=30
DB_LOGIN_TIMEOUT=15

# MCP server options
MCP_HTTP=1         # 1 = HTTP (default), 0 = STDIO
MCP_HOST=127.0.0.1 # host for HTTP server
MCP_PORT=8000      # port for HTTP server
🚀 Running
Start the MCP server:

bash
Copy code
python sql_mcp.py
Modes
HTTP (default): server runs at http://127.0.0.1:8000/mcp

STDIO: set MCP_HTTP=0 (for clients like Claude Desktop)

🔧 Available Tools
Tool	Description
refresh_schema()	Reloads schema cache (tables, columns, objects, jobs)
get_table_schema(table)	Returns schema of a given table (columns + types)
get_column_data(table, select_col, where_col, value)	Sample values from a column with filter
get_column_population_logic(column)	Find procedures that insert/update a column
get_object_definition(object)	Returns SQL definition of an object (procedure, view, etc.)
get_job_status(job)	Returns last run status of a SQL Agent job

📚 Resources
sql://index — overview of tables and jobs

sql://tables — markdown list of all base tables

sql://jobs — markdown list of SQL Agent jobs

🧩 Example Usage
python
Copy code
from mcp import MCPClient

client = MCPClient("http://127.0.0.1:8000/mcp")

# Refresh cache
client.call("refresh_schema")

# Get schema for 'Employees' table
schema = client.call("get_table_schema", {"table": "Employees"})
print(schema)

# Check job status
status = client.call("get_job_status", {"job": "Nightly ETL"})
print(status)
🗂️ Architecture
text
Copy code
+---------------------+        HTTP/STDIO        +----------------------+
|   MCP-Compatible    | <----------------------> |   SQL MCP Tool        |
|   Client / Agent    |                          | (FastMCP server)      |
|   (e.g. Copilot)    |                          |                        |
+---------------------+                          +----------------------+
                                                        |
                                                        | pyodbc (ODBC Driver)
                                                        v
                                               +----------------------+
                                               |   SQL Server DB      |
                                               |  (tables, jobs,      |
                                               |   procs, views)      |
                                               +----------------------+
Client/Agent: Your AI assistant, copilot, or tool that speaks MCP

SQL MCP Tool: This project — exposes DB metadata & jobs via FastMCP

SQL Server: The actual database, queried securely via pyodbc

📦 Deployment Notes
Recommended to run inside a virtual environment.

For secure remote access, you can tunnel the HTTP server with cloudflared or ngrok.

Use least-privilege DB credentials (read-only if you just need metadata).

📝 License
MIT License. See LICENSE for details.

📣 Credits
Built on FastMCP

Uses pyodbc for SQL Server connectivity