import asyncio
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

# --- EDIT THESE ---
PY = r"C:\Users\shivam.mishra\AppData\Local\Programs\Python\Python313\python.exe"
SERVER = r"C:\Users\shivam.mishra\Python learning\Sql_Server_McpServer\sql_mcp_minimal.py"
ENV = {
    "DB_SERVER": "8RBQW14-PC",
    "DB_NAME": "TestDatabase",
    "DB_USER": "readonly_agent",
    "DB_PASS": "Phenom@21"
}
TEST_TABLE = "Departments"  # put a real table name here
# ------------------

async def main():
    params = StdioServerParameters(command=PY, args=[SERVER], env=ENV)
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            tools = await session.list_tools()
            print("TOOLS:", [t.name for t in tools.tools])

            res = await session.call_tool("refresh_schema", {})
            print("REFRESH:", res.content if hasattr(res, "content") else res)

            res = await session.call_tool("get_table_schema", {"table": TEST_TABLE})
            print("SCHEMA:", res.content if hasattr(res, "content") else res)

            md = await session.read_resource("sql://index")
            print("INDEX:\n", (md[:300] + "…") if isinstance(md, str) else md)

if __name__ == "__main__":
    asyncio.run(main())
