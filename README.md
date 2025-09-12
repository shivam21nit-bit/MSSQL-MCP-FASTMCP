# SQL MCP Tool

A **FastAPI-based metadata querying tool** that conforms to the [GitMCP](https://gitmcp.io) standard.  
This tool exposes APIs for interacting with SQL Server metadata such as table schemas, column data, job statuses, and object definitions.

---

## 🧰 Tool Description

This repository provides a server that supports structured metadata queries against a SQL Server database.  
It is designed to be easily integrated with large language model (LLM) agents using the [GitMCP Tool Use Protocol](https://github.com/microsoft/gitmcp).

---

## 🚀 Available Endpoints

### `POST /v1/tool-use`

Trigger execution of any supported tool by specifying the `tool` name and required `parameters`.

### `GET /v1/metadata`

Returns metadata about all available tools and expected input parameters.

---

## 🛠️ Tools

### 1. `get_column_data`

**Description**: Fetch data from a specific column with a `WHERE` filter.

**Parameters**:
```json
{
  "table": "Name of the table",
  "select_col": "Column to select",
  "where_col": "Column to filter on",
  "value": "Value for the filter"
}
````

---

### 2. `get_column_population_logic`

**Description**: Returns stored procedures that populate the specified column (via INSERT or UPDATE).

**Parameters**:

```json
{
  "column": "Name of the column"
}
```

---

### 3. `get_table_schema`

**Description**: Returns column names and data types of the specified table.

**Parameters**:

```json
{
  "table": "Name of the table"
}
```

---

### 4. `get_object_definition`

**Description**: Returns SQL definition (source code) of a stored procedure or view.

**Parameters**:

```json
{
  "object": "Name of the object"
}
```

---

### 5. `get_job_status`

**Description**: Returns last known run status of a SQL Server Agent job.

**Parameters**:

```json
{
  "job": "Name of the job"
}
```

---

## ⚙️ Setup Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/<your-username>/<your-repo>.git
cd <your-repo>
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

> ⚠️ Ensure you have `ODBC Driver 17 for SQL Server` installed.

### 3. Run the Server

```bash
uvicorn main:app --reload
```

* By default, server runs at:
  **[http://localhost:8000](http://localhost:8000)**
* Swagger UI available at:
  **[http://localhost:8000/docs](http://localhost:8000/docs)**

---

## 🤖 GitMCP Compatibility

This repository is compatible with [GitMCP](https://gitmcp.io), enabling LLMs to discover and use tools via:

```
GET https://gitmcp.io/<your-username>/<your-repo>/v1/metadata
POST https://gitmcp.io/<your-username>/<your-repo>/v1/tool-use
```

Ensure the following:

* `README.md` and `llms.txt` are in the **root** directory.
* Server supports `/v1/metadata` and `/v1/tool-use` endpoints.

---

## 📂 Example `llms.txt` (include in root of repo)

```
/v1/metadata
/v1/tool-use
```

---

## 📌 Requirements

* Python 3.8+
* FastAPI
* pyodbc
* Uvicorn
* SQL Server ODBC Driver
