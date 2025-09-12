# SQL MCP Tool

A FastAPI-based **MCP-compliant** tool for querying Microsoft SQL Server metadata. This tool is intended to be run by an agent within a Model Context Protocol (MCP) environment and responds to structured tool-use queries.

---

## 🚀 Features

- 🔍 Fetch column data with WHERE filters
- 📊 Retrieve full table schema (columns + types)
- 🧠 Discover stored procedures that populate a specific column
- 📄 View SQL definition of objects (stored procedures / views)
- 🕒 Get last run status of SQL Server Agent jobs

---

## 🧰 Endpoints

### `POST /v1/tool-use`

Use this endpoint to trigger any of the supported tools.

#### Example Request:

```json
{
  "tool": "get_table_schema",
  "parameters": {
    "table": "Customers"
  }
}

Tools Supported:
Tool Name	Description
get_column_data	Fetch values from a column with a WHERE filter
get_column_population_logic	Find procedures that populate a given column
get_table_schema	Retrieve column names and data types for a table
get_object_definition	Get SQL definition of a view or stored procedure
get_job_status	Get the last execution status of a SQL Server Agent job
GET /v1/metadata

Returns tool metadata in a format compatible with MCP registries.

🛠️ Running Locally
🔧 1. Install Dependencies
pip install -r requirements.txt

▶️ 2. Start the API Server
uvicorn final_mcp:app --host 0.0.0.0 --port 8000 --reload

⚙️ Configuration

Update the DB_CONFIG dictionary inside final_mcp.py with your SQL Server details:

DB_CONFIG = {
    'server': 'YOUR_SERVER',
    'database': 'YOUR_DATABASE',
    'username': 'YOUR_USERNAME',
    'password': 'YOUR_PASSWORD',
    'driver': '{ODBC Driver 17 for SQL Server}',
}


✅ Note: For security, consider moving credentials to a .env file in production.

📁 Project Structure
Sql_Server_McpServer/
├── final_mcp.py           # Main FastAPI application
├── requirements.txt       # Dependencies
├── README.md              # This file
├── .gitignore             # Git ignore rules
└── mcp_metadata.json      # MCP-compatible tool metadata

✅ MCP Compliance Notes

The tool is compatible with MCP Git-based tool runners.

It exposes:

/v1/tool-use: Accepts tool+parameters and returns results

/v1/metadata: Describes the tool’s capabilities and parameters

Ensure the repository is accessible to your MCP agent.

🔒 Security

❗ Never hardcode production credentials.

Use environment variables or secret managers where possible.

Consider using python-dotenv or FastAPI's Settings class for config management.

🧪 Testing

You can test locally using tools like Postman or cURL:

curl -X POST http://localhost:8000/v1/tool-use \
     -H "Content-Type: application/json" \
     -d '{
           "tool": "get_table_schema",
           "parameters": {
               "table": "Customers"
           }
         }'

🧑‍💻 Author

Built by [Shivam Mishra], 2025. Intended for MCP environments and enterprise SQL auditing tools.