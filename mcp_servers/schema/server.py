
import sqlite3
from pathlib import Path
from typing import Optional
from mcp.server.fastmcp import FastMCP

mcp_server = FastMCP("VeriSQL-Schema-MCP")

DATABASES = {"verisql_demo": Path(__file__).resolve().parents[2] / "data" / "verisql_demo.db"}
SENSITIVE_COLUMNS = {"email", "phone", "password", "token", "ssn", "credit_card"}

def get_connection(db_id: str):
    if db_id not in DATABASES:
        raise ValueError("Unauthorised database.")

    path = DATABASES[db_id].resolve()
    if not path.is_file():
        raise FileNotFoundError("Database file not found.")

    connection = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True, timeout=5)
    connection.execute("PRAGMA query_only = ON")
    return connection

@mcp_server.tool()
def list_databases() -> list[str]:
    """List authorised databases."""
    return list(DATABASES.keys())

@mcp_server.tool()
def get_schema(db_id: str, tables: Optional[list[str]] = None) -> dict:
    """Retrieve table columns, primary keys and foreign keys."""
    connection = get_connection(db_id)
    try:
        available = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")}
        selected = sorted(available if tables is None else set(tables))
        if not set(selected).issubset(available):
            raise ValueError("Unknown table requested.")

        result = {}
        for table in selected:
            columns = connection.execute(f'PRAGMA table_info("{table}")').fetchall()
            foreign_keys = connection.execute(f'PRAGMA foreign_key_list("{table}")').fetchall()
            result[table] = {
                "columns": [{"name": c[1], "type": c[2], "primary_key": bool(c[5]), "nullable": not bool(c[3]), "sensitive": c[1].lower() in SENSITIVE_COLUMNS} for c in columns],
                "foreign_keys": [{"column": f[3], "referenced_table": f[2], "referenced_column": f[4]} for f in foreign_keys]
            }
        return result
    finally:
        connection.close()

@mcp_server.tool()
def sample_values(db_id: str, table: str, column: str, n: int = 5) -> list:
    """Retrieve limited distinct values from non-sensitive columns."""
    if not 1 <= n <= 10:
        raise ValueError("n must be between 1 and 10.")

    schema = get_schema(db_id, [table])
    columns = {c["name"] for c in schema[table]["columns"]}
    if column not in columns:
        raise ValueError("Unknown column.")
    if column.lower() in SENSITIVE_COLUMNS:
        raise ValueError("Sensitive column samples are restricted.")

    connection = get_connection(db_id)
    try:
        query = f'SELECT DISTINCT "{column}" FROM "{table}" WHERE "{column}" IS NOT NULL LIMIT ?'
        return [row[0] for row in connection.execute(query, (n,)).fetchall()]
    finally:
        connection.close()

@mcp_server.tool()
def find_relevant_columns(db_id: str, question: str, k: int = 10) -> list[dict]:
    """Find relevant columns using names, table context and database values."""
    if not question.strip() or len(question) > 1000:
        raise ValueError("Invalid question.")
    if not 1 <= k <= 30:
        raise ValueError("k must be between 1 and 30.")

    def normalize(value):
        words = "".join(c.lower() if c.isalnum() else " " for c in str(value)).split()
        return {word[:-1] if len(word) > 3 and word.endswith("s") else word for word in words}

    schema = get_schema(db_id)
    terms = normalize(question)
    matches = []
    connection = get_connection(db_id)

    try:
        for table, details in schema.items():
            table_score = len(normalize(table) & terms) * 2

            for column in details["columns"]:
                name = column["name"]
                if column["sensitive"]:
                    continue

                score = table_score + len(normalize(name.replace("_", " ")) & terms) * 3
                quoted_table = '"' + table.replace('"', '""') + '"'
                quoted_column = '"' + name.replace('"', '""') + '"'
                query = f"SELECT DISTINCT {quoted_column} FROM {quoted_table} WHERE {quoted_column} IS NOT NULL LIMIT 100"

                for row in connection.execute(query).fetchall():
                    value = normalize(row[0])
                    if value and value.issubset(terms):
                        score += 5
                        break

                if score > 2:
                    matches.append({"table": table, "column": name, "score": score})
    finally:
        connection.close()

    return sorted(matches, key=lambda item: (-item["score"], item["table"], item["column"]))[:k]
if __name__ == "__main__":
    mcp_server.run(transport="stdio")
