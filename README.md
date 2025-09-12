Great — since your `llms.txt` now correctly points to:

```
https://raw.githubusercontent.com/shivam21nit-bit/Sql-Server-MCP/main/README.md
```

I'll now generate a clean and GitMCP-compatible `README.md` for your project based on your `final_mcp.py` script.

---

## ✅ `README.md` (Markdown Format)

````markdown
# SQL MCP Tool

**Version:** 1.1.0  
**Author:** [shivam21nit-bit](https://github.com/shivam21nit-bit)  
**Description:** A FastAPI-based MCP-compliant server for querying SQL Server metadata.

---

## 🧩 Overview

This project implements a [Model Context Protocol (MCP)](https://github.com/modelcontextprotocol) server to expose structured metadata from a SQL Server instance using a set of well-defined tools.

It supports use cases such as:
- Inspecting table schemas
- Checking job statuses
- Retrieving stored procedure definitions
- Exploring column population logic

---

## 🚀 Endpoints

### `POST /v1/tool-use`

Use this endpoint to run one of the supported tools.

#### Request Format:

```json
{
  "tool": "tool_name",
  "parameters": {
    "key": "value"
  }
}
````

---

## 🔧 Supported Tools

### 1. `get_column_data`

Fetch data from a column using a `WHERE` condition.

**Parameters:**

* `table`: Name of the table
* `select_col`: Column to select
* `where_col`: Column to filter on
* `value`: Value for the filter

---

### 2. `get_column_population_logic`

Find stored procedures that update or insert into a given column.

**Parameters:**

* `column`: Name of the column

---

### 3. `get_table_schema`

Return all columns and data types of a table.

**Parameters:**

* `table`: Name of the table

---

### 4. `get_object_definition`

Fetch the SQL definition of a stored procedure or view.

**Parameters:**

* `object`: Name of the object

---

### 5. `get_job_status`

Get the last run status of a SQL Server Agent job.

**Parameters:**

* `job`: Name of the job

---

## 🗂️ Metadata Endpoint

### `GET /v1/metadata`

Returns structured metadata about the tools exposed by this server.

---

## ♻️ Refresh Schema Cache

### `POST /v1/refresh-schema`

Rebuilds the in-memory cache of tables, columns, jobs, and objects.

---

## 📦 Environment Variables

You should create a `.env` file in the root directory with the following:

```env
DB_SERVER=your_sql_server
DB_NAME=your_database_name
DB_USER=readonly_user
DB_PASS=your_password
```

---

## 📥 Installation & Setup

```bash
# Clone the repo
git clone https://github.com/shivam21nit-bit/Sql-Server-MCP.git
cd Sql-Server-MCP

# Create virtual environment (optional)
python -m venv venv
source venv/bin/activate   # or .\venv\Scripts\activate on Windows

# Install dependencies
pip install -r requirements.txt

# Start the server
uvicorn final_mcp:app --reload
```

---

## 📝 Requirements

The dependencies are listed in `requirements.txt` and include:

* fastapi
* uvicorn
* pyodbc
* python-dotenv
* pydantic

---

## 🧠 llms.txt

This repository includes a `llms.txt` pointing to the raw `README.md` file so that GitMCP can discover the tool documentation.

```
https://raw.githubusercontent.com/shivam21nit-bit/Sql-Server-MCP/main/README.md
```

---

## 📄 License

MIT License © 2025 shivam21nit-bit

```
