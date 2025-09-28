DOTA — Data Origin & Traceability Assistant (FastMCP)

What it is: An MCP server that helps you discover where data comes from and how it’s populated in SQL Server—and report SQL Agent job status/failures—using natural-language prompts.
What you get: Clear text answers plus a client-friendly topology JSON you can render as a diagram (no Graphviz required).

🚀 Capabilities (at a glance)

Ask in natural language

“Which table has column Salary?”

“How is column Salary populated?”

“What are the status of the jobs?”

“What is the reason of failure of job PayrollLoad?”

Column discovery

Finds all tables (base tables by default) that contain a given column.

Disambiguates when multiple tables match (uses writer count, triggers, schema preference, name heuristics, and row counts).

Population logic tracing

Detects writers via static parsing of:

UPDATE … SET

INSERT … SELECT

INSERT … VALUES

MERGE … UPDATE / MERGE … INSERT

Flags dynamic SQL heuristically (sp_executesql / EXEC).

Returns procedure snippets/excerpts pinpointing the assignment expression (e.g., Salary = @NewSalary).

Includes computed column and default constraint definitions if present.

Topology for diagrams

Returns a lightweight graph (nodes[] + edges[]) that UIs can render directly as SVG/Canvas (no Graphviz, no extra deps).

SQL Agent job visibility

Latest job status, whether a job is currently running, and last failure details (time, step id/name, message).

Synonym-aware + cache-backed

Resolves synonyms and warms an in-memory schema/procedure/dependency cache for fast queries.

Privacy by default

Responses include database name only (server hidden) unless you change a single env flag.

Dynamic connection switching

Change servers/databases at runtime via a tool call; optional safe persistence to .env.

🧰 Tools (high level)

Connection & config

test_connection, connect_db, current_connection, list_env_defaults

Schema & permissions

refresh_schema, permissions_self_test, get_table_schema, get_object_definition

Data sample

get_column_data (quick peek of values with a filter)

Discovery / lineage / population

find_tables_with_column

get_column_lineage (graph of logic)

get_column_population (lineage + topology JSON + writer snippets)

Natural-language wrappers

ask_where_column(prompt) → “which table has column Salary?”

ask_column_population(prompt) → “how is salary populated?”
(Auto-selects the best table if multiple match; returns alternatives when tied.)

Jobs

get_jobs_overview, ask_jobs(prompt) → status of all jobs, a single job, or failure reasons within a lookback window.

📦 Requirements

Python 3.10+

SQL Server ODBC driver (e.g., ODBC Driver 17 or 18 for SQL Server)

Python packages:

pip install fastmcp python-dotenv pyodbc
# optional for tunneling:
# pip install cloudflared


If you see 08001 / “driver not found”, install Microsoft’s ODBC Driver and ensure the DB_DRIVER env matches it.

⚙️ Configuration (env vars)

Create a local .env (not committed) with no secrets checked in:

# Connection (you can also switch dynamically via the connect_db tool)
DB_SERVER=
DB_NAME=
DB_USER=
DB_PASS=
DB_DRIVER={ODBC Driver 17 for SQL Server}
DB_TIMEOUT=30
DB_LOGIN_TIMEOUT=15

# Server transport
MCP_HTTP=1
MCP_HOST=127.0.0.1
MCP_PORT=8000

# Behavior & safety
DOTA_EXPOSE_DATABASE=1          # 1 = return DB name only (hides server)
DOTA_INCLUDE_DEFS=excerpt       # none | excerpt | full (procedure text in results)
LINEAGE_MAX_DEPTH=5             # default lineage depth (hard cap is 10)
MAX_PROC_SCAN=3000              # fallback “scan all procs” cap
DB_THREADLOCAL=0                # 1 = reuse a thread-local connection (perf)

🏁 Run the server
python main.py


HTTP: starts at http://127.0.0.1:8000/mcp/

STDIO: set MCP_HTTP=0 to run over stdio

💬 Use with Claude Desktop

Claude Desktop supports MCP over HTTP.

Start this server: python main.py

In Claude Desktop → Settings → Tools / MCP / Connections:

Add HTTP MCP Server

URL: http://127.0.0.1:8000/mcp/

Save/Enable

Chat with Claude and just ask:

“Which table has column Salary?”

“How is column Salary populated?”

“What are the status of the jobs?”

“What is the reason of failure of job PayrollLoad?”

Claude will call the right tools and show answers. For visuals, Claude (or your own UI) can render the topology JSON into a diagram.

Any UI can turn that into an inline SVG. For example, a minimal browser snippet can lay out nodes in layers and draw arrows (no libraries needed). You can reuse your own renderer or the simple example you already have in your project.

If you prefer, you can later add a server option (e.g., render_svg=true) to return a ready-made SVG string for instant display.

🧪 Common prompts & what happens

“Which table has column Salary?”
→ ask_where_column searches metadata (plus cache), returns all tables with that column.

“How is column Salary populated?”
→ ask_column_population:

If there’s one table → returns writers (procedures/triggers), snippets, expressions, computed/default info, and topology.

If multiple tables → auto-picks the best match (writers/trigger count, dbo preference, name heuristics, rowcount). Includes alternatives if tied.

“What are the status of the jobs?” / “reason of failure of job X?”
→ ask_jobs:

Shows latest status across jobs (or for a single job).

Includes running indicator and last failure time + step details + message (within a configurable lookback window).

🔒 Security & privacy

No secrets in Git – Keep .env local and untracked.

Server name hidden by default – Only the database name appears in responses (DOTA_EXPOSE_DATABASE=1).

Uses READ UNCOMMITTED for metadata lookups to reduce blocking.

Data samples are explicit (via get_column_data), never implicit.

🛠️ Troubleshooting

ODBC / connection errors (08001, timeouts):

Ensure the SQL Server is reachable, remote connections allowed, firewall open.

Verify the ODBC driver is installed and DB_DRIVER matches the installed driver name.

“Object not found” when fetching definitions:

Try the qualified name (dbo.ProcName) or run refresh_schema first.

The tool also attempts cache-cold resolution via sys.objects.

Population shows no writers:

Check permissions_self_test (you may lack visibility to sys.sql_modules or sys.sql_expression_dependencies).

Writers that use dynamic SQL are best-effort and may need manual review.

📈 Design notes (how it’s fast & helpful)

Warm caches for tables/columns/objects/procedures/dependencies & synonyms.

Reverse dependency index speeds “who writes this table?” discovery.

Robust parsing for common write patterns + dynamic SQL heuristic.

Client-friendly graph: tiny nodes/edges structure that any UI can draw.

🗺️ Roadmap ideas

Optional render_svg flag to return an SVG alongside topology JSON.

Parameter origin tracing (follow where @NewSalary comes from).

Richer edge labels (join/filter context where feasible).

Paging & filters for job views.

📜 License

MIT (or your chosen license).

🤝 Contributing

Issues and PRs welcome. Please do not include real server names, credentials, or organization-specific sensitive details in examples.