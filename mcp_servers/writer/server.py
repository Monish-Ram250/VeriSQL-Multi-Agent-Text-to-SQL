
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from mcp.server.fastmcp import FastMCP
from agents.writer import generate_sql as writer_generate_sql
from agents.writer import repair_sql as writer_repair_sql

mcp_server = FastMCP("VeriSQL Writer MCP")

@mcp_server.tool()
async def generate_sql(question: str, db_id: str = "verisql_demo") -> dict:
    """Generate SQLite SQL from a natural-language question."""
    result = await writer_generate_sql(question, db_id)
    return result.model_dump()

@mcp_server.tool()
async def repair_sql(question: str, previous_sql: str, evaluator_feedback: str, db_id: str = "verisql_demo") -> dict:
    """Repair SQL using Evaluator Agent feedback."""
    result = await writer_repair_sql(question, previous_sql, evaluator_feedback, db_id)
    return result.model_dump()

if __name__ == "__main__":
    mcp_server.run(transport="stdio")
