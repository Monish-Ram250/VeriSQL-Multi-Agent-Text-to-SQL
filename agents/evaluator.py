
import sqlite3
from pathlib import Path

from sqlglot import parse, exp


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATABASE_PATH = PROJECT_ROOT / "data" / "verisql_demo.db"


def validate_sql_syntax(sql_query: str) -> dict:
    """Validate SQL syntax and ensure exactly one query is provided."""

    try:
        parsed_queries = parse(sql_query, read="sqlite")

        if len(parsed_queries) != 1 or parsed_queries[0] is None:
            return {"valid": False, "error": "Only one SQL statement is allowed."}

        if not isinstance(parsed_queries[0], exp.Query):
            return {"valid": False, "error": "Only SELECT queries are allowed."}

        return {"valid": True, "error": None}

    except Exception as error:
        return {"valid": False, "error": str(error)}


def validate_sql_safety(sql_query: str) -> dict:
    """Reject modifying operations and access to sensitive columns."""

    syntax_result = validate_sql_syntax(sql_query)

    if not syntax_result["valid"]:
        return syntax_result

    query = parse(sql_query, read="sqlite")[0]

    restricted_operations = (
        exp.Insert, exp.Update, exp.Delete,
        exp.Create, exp.Drop, exp.Alter, exp.Command
    )

    for operation in restricted_operations:
        if query.find(operation):
            return {
                "valid": False,
                "error": f"Restricted SQL operation: {operation.__name__}"
            }

    sensitive_keywords = {
        "email", "phone", "password", "token",
        "ssn", "credit_card"
    }

    for column in query.find_all(exp.Column):
        if column.name.lower() in sensitive_keywords:
            return {
                "valid": False,
                "error": f"Access to sensitive column '{column.name}' is restricted."
            }

    return {"valid": True, "error": None}


def validate_sql_schema(sql_query: str) -> dict:
    """Check referenced tables and columns against the SQLite database."""

    syntax_result = validate_sql_syntax(sql_query)

    if not syntax_result["valid"]:
        return syntax_result

    connection = None

    try:
        query = parse(sql_query, read="sqlite")[0]

        database_uri = (
            f"file:{DATABASE_PATH.resolve().as_posix()}?mode=ro"
        )

        connection = sqlite3.connect(database_uri, uri=True)
        connection.execute("PRAGMA query_only = ON")
        connection.execute(
            "EXPLAIN " + query.sql(dialect="sqlite")
        )

        return {"valid": True, "error": None}

    except sqlite3.Error as error:
        return {"valid": False, "error": str(error)}

    finally:
        if connection is not None:
            connection.close()


def validate_sql_execution(sql_query: str, max_rows: int = 20) -> dict:
    """Execute SQL using a read-only connection and return limited results."""

    safety_result = validate_sql_safety(sql_query)

    if not safety_result["valid"]:
        return {
            "valid": False,
            "error": safety_result["error"],
            "columns": [],
            "rows": []
        }

    connection = None

    try:
        database_uri = (
            f"file:{DATABASE_PATH.resolve().as_posix()}?mode=ro"
        )

        connection = sqlite3.connect(database_uri, uri=True)
        connection.execute("PRAGMA query_only = ON")

        query = parse(sql_query, read="sqlite")[0]
        clean_sql = query.sql(dialect="sqlite")

        cursor = connection.cursor()
        cursor.execute(
            f"SELECT * FROM ({clean_sql}) LIMIT ?",
            (max_rows,)
        )

        rows = cursor.fetchall()
        columns = [item[0] for item in cursor.description]

        return {
            "valid": True,
            "error": None,
            "columns": columns,
            "rows": rows
        }

    except sqlite3.Error as error:
        return {
            "valid": False,
            "error": str(error),
            "columns": [],
            "rows": []
        }

    finally:
        if connection is not None:
            connection.close()
