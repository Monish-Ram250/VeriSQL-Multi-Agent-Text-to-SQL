
from pathlib import Path
import sys

# Add project root to Python path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from mcp.server.fastmcp import FastMCP
from agents.evaluator import (
    validate_sql_syntax,
    validate_sql_safety,
    validate_sql_schema,
    validate_sql_execution
)

mcp_server = FastMCP("VeriSQL-Verifier")


@mcp_server.tool()
def check_sql_syntax(sql_query: str) -> dict:
    """Check SQL syntax and statement count."""
    return validate_sql_syntax(sql_query)


@mcp_server.tool()
def check_sql_safety(sql_query: str) -> dict:
    """Check read-only restrictions and sensitive column access."""
    return validate_sql_safety(sql_query)


@mcp_server.tool()
def check_sql_schema(sql_query: str) -> dict:
    """Check referenced database tables and columns."""
    return validate_sql_schema(sql_query)


@mcp_server.tool()
def execute_sql_readonly(sql_query: str) -> dict:
    """Execute a permitted SQL query using a read-only connection."""
    return validate_sql_execution(sql_query)


if __name__ == "__main__":
    mcp_server.run(transport="stdio")
