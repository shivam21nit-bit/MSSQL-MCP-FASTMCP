## 📘 **SQL MCP Tool — README for Agents**

### ✅ **Overview**

This is an MCP-compliant tool that allows querying SQL Server metadata using structured API requests. The tool provides multiple endpoints to fetch information such as table schemas, column data, object definitions, and job statuses. The tool uses a read-only database connection and relies on cached metadata to serve requests efficiently.

---

### ⚙ **Setup Instructions**

1. **Database Connection**
   The tool connects to a SQL Server instance using the following read-only credentials:

   ```python
   DB_CONFIG = {
       'server': '8RBQW14-PC',
       'database': 'TestDatabase',
       'username': 'readonly_agent',
       'password': 'Phenom@21',
       'driver': '{ODBC Driver 17 for SQL Server}',
   }
   ```

   ➤ Ensure that the `readonly_agent` account has permissions only for SELECT operations.

2. **Schema Cache**
   Upon startup, the tool loads metadata from the database into memory, including:

   * Tables and their columns
   * Procedures and views
   * SQL Agent jobs

---

### 🚀 **Available Endpoints**

### 📥 **1. `/v1/tool-use` (POST)**

Use this endpoint to execute one of the available tools by providing its name and parameters.

**Request Body Example:**

```json
{
  "tool": "get_column_data",
  "parameters": {
    "table": "Employees",
    "select_col": "Name",
    "where_col": "DepartmentID",
    "value": "3"
  }
}
```

#### ✅ Tools Available:

1. **get\_column\_data**
   Fetch records from a specific column filtered by a WHERE condition.

2. **get\_column\_population\_logic**
   Retrieve procedures that populate a given column via INSERT or UPDATE statements.

3. **get\_table\_schema**
   Get the column names and data types for a specific table.

4. **get\_object\_definition**
   Retrieve the definition of a stored procedure or view.

5. **get\_job\_status**
   Get the latest run status of a SQL Agent job.

---

### 📦 **2. `/v1/metadata` (GET)**

Returns details about the tool and its available operations.

**Response Example:**

```json
{
  "name": "sql_mcp_tool",
  "description": "Tool for querying SQL Server metadata using structured parameters.",
  "tools": [
    {
      "tool": "get_column_data",
      "description": "Fetch data from a specific column with a WHERE condition.",
      "parameters": { "table": "...", "select_col": "...", "where_col": "...", "value": "..." }
    },
    ...
  ]
}
```

---

### ✅ **Usage Notes for Agents**

* All requests should be sent as JSON.
* Use the `/v1/metadata` endpoint to discover available tools and required parameters.
* The tool only supports queries that retrieve information. No insert, update, or delete operations are allowed through this interface.
* Table, column, object, and job names are case-insensitive but should be validated against cached metadata.
* All errors are communicated with proper HTTP status codes:

  * `400` for missing or invalid parameters
  * `404` for objects not found
  * `500` for server or connection errors

---

### 🔑 **Security Considerations**

* The tool uses a read-only database account.
* All operations are restricted to queries with no modification rights.
* Access logs are captured for debugging and monitoring.

---

### 📂 **Directory Structure**

```
/
├── final_mcp.py               # FastAPI application and endpoints
├── requirements.txt      # Python dependencies
├── README.md             # This guide for agents
```

---

### 📬 **Contact**

For assistance or further details, reach out to the system administrator or database team managing the SQL Server instance.

