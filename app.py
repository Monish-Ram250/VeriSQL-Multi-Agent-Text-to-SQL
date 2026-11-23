"""VeriSQL web application: the complete backend in a single file.

Architecture (preserved from the notebooks):

    Question -> Memory retrieval -> Writer Agent (via Writer MCP, which calls
    Schema MCP) -> Evaluator Agent (via Verifier MCP) -> accept / repair / stop
    -> read-only execution -> long-term memory.

The three MCP servers are hosted inside this file and connected to their
clients through the MCP SDK's in-memory transport. Each server is built per
database session, so no credentials ever travel through argv, environment
variables or subprocesses.

LangSmith is deliberately not used anywhere in this application.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import sqlite3
import tempfile
import threading
import time
import uuid
from contextlib import asynccontextmanager
from datetime import date, datetime, time as dtime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, AsyncIterator, Literal, Optional, TypedDict

import numpy as np
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, File, Form, Request, Response, UploadFile
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langgraph.graph import END, START, StateGraph
from mcp.server.fastmcp import FastMCP
from mcp.shared.memory import create_connected_server_and_client_session
from pydantic import BaseModel, Field, SecretStr, ValidationError, field_validator
from sqlglot import exp, parse

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")


def _env_int(name: str, default: int) -> int:
    """Read an integer environment variable, falling back to a default."""
    try:
        return int(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


DATA_DIR = Path(os.getenv("VERISQL_DATA_DIR", BASE_DIR / "data")).resolve()
UPLOAD_DIR = DATA_DIR / "uploads"
FRONTEND_DIR = BASE_DIR / "frontend"
MEMORY_DB_PATH = DATA_DIR / "verisql_memory.db"

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
WRITER_MODEL = os.getenv("VERISQL_WRITER_MODEL", "openai/gpt-oss-20b")
EVALUATOR_MODEL = os.getenv("VERISQL_EVALUATOR_MODEL", "openai/gpt-oss-20b")
LLM_TIMEOUT_SECONDS = _env_int("VERISQL_LLM_TIMEOUT", 60)

EMBEDDING_MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
EMBEDDING_DIMENSION = 384

MAX_REPAIR_ATTEMPTS = 3
MAX_RESULT_ROWS = _env_int("VERISQL_MAX_RESULT_ROWS", 200)
QUERY_TIMEOUT_SECONDS = _env_int("VERISQL_QUERY_TIMEOUT", 10)
MAX_UPLOAD_BYTES = _env_int("VERISQL_MAX_UPLOAD_MB", 50) * 1024 * 1024
SESSION_TTL_SECONDS = _env_int("VERISQL_SESSION_TTL_MINUTES", 60) * 60
EVALUATOR_PREVIEW_ROWS = _env_int("VERISQL_EVALUATOR_PREVIEW_ROWS", 5)
MAX_SQL_LENGTH = 20_000
MAX_CONTEXT_TABLES = 40
SESSION_COOKIE = "verisql_session"

DialectName = Literal["sqlite", "postgresql", "mysql"]
SQLGLOT_DIALECT = {"sqlite": "sqlite", "postgresql": "postgres", "mysql": "mysql"}
DIALECT_LABEL = {"sqlite": "SQLite", "postgresql": "PostgreSQL", "mysql": "MySQL"}

# Column-name policy carried over from the original Schema MCP and Evaluator.
SENSITIVE_COLUMNS = {"email", "phone", "password", "token", "ssn", "credit_card"}
SENSITIVE_SUBSTRINGS = ("password", "passwd", "secret", "api_key", "credit_card", "card_number")
SYSTEM_SCHEMAS = {"information_schema", "pg_catalog", "mysql", "performance_schema", "sys"}
DANGEROUS_FUNCTIONS = {
    "sleep", "benchmark", "load_file", "load_extension", "readfile", "writefile",
    "dblink", "dblink_exec", "set_config", "pg_sleep", "pg_read_file",
    "pg_read_binary_file", "pg_ls_dir", "lo_import", "lo_export",
}
# Statement and clause types that can modify data, schema or server state.
BLOCKED_NODES = tuple(
    getattr(exp, name)
    for name in (
        "Insert", "Update", "Delete", "Create", "Drop", "Alter", "Command", "Merge",
        "Into", "Lock", "Copy", "Pragma", "Set", "Use", "Grant", "TruncateTable",
        "Transaction", "Commit", "Rollback", "Attach", "Detach", "Analyze", "Kill",
    )
    if hasattr(exp, name)
)

# ---------------------------------------------------------------------------
# Logging with credential redaction
# ---------------------------------------------------------------------------

_SECRETS: dict[str, int] = {}  # Reference-counted: two sessions may share a password.
_SECRETS_LOCK = threading.Lock()


def register_secret(value: Optional[str]) -> None:
    """Remember a secret so that it can be scrubbed from logs and errors."""
    if value and len(value) >= 3:
        with _SECRETS_LOCK:
            _SECRETS[value] = _SECRETS.get(value, 0) + 1


def unregister_secret(value: Optional[str]) -> None:
    """Release one reference to a secret once its session has ended."""
    if not value:
        return
    with _SECRETS_LOCK:
        remaining = _SECRETS.get(value, 0) - 1
        if remaining > 0:
            _SECRETS[value] = remaining
        else:
            _SECRETS.pop(value, None)


def scrub(text: Any) -> str:
    """Mask any registered secret in a piece of text."""
    result = str(text)
    with _SECRETS_LOCK:
        for secret in sorted(_SECRETS, key=len, reverse=True):
            result = result.replace(secret, "***")
    return result


class RedactingFilter(logging.Filter):
    """Logging filter that removes credentials from every record."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = scrub(record.getMessage())
        record.args = ()
        return True


logging.basicConfig(
    level=os.getenv("VERISQL_LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
for _handler in logging.getLogger().handlers:
    _handler.addFilter(RedactingFilter())
logger = logging.getLogger("verisql")


# ---------------------------------------------------------------------------
# Errors and small helpers
# ---------------------------------------------------------------------------

class ApiError(Exception):
    """An error that is safe to show to the client."""

    def __init__(self, status: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message


class LLMServiceError(Exception):
    """Raised when the language model could not be reached or understood."""


def to_jsonable(value: Any) -> Any:
    """Convert database values into JSON-safe values."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date, dtime)):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray, memoryview)):
        return f"<binary {len(bytes(value))} bytes>"
    if isinstance(value, (list, tuple, set)):
        return [to_jsonable(item) for item in value]
    if isinstance(value, dict):
        return {str(key): to_jsonable(item) for key, item in value.items()}
    return str(value)


def is_sensitive_column(name: str) -> bool:
    """Return True when a column name falls under the sensitive-data policy."""
    lowered = name.lower()
    return lowered in SENSITIVE_COLUMNS or any(part in lowered for part in SENSITIVE_SUBSTRINGS)


def quote_identifier(name: str, dialect: str) -> str:
    """Quote an identifier correctly for the target dialect."""
    return exp.to_identifier(name, quoted=True).sql(dialect=SQLGLOT_DIALECT[dialect])


# ---------------------------------------------------------------------------
# Database session: connection, read-only access, introspection
# ---------------------------------------------------------------------------

class ConnectionRequest(BaseModel):
    """Validated connection details submitted by the user."""

    db_type: DialectName
    host: Optional[str] = None
    port: Optional[int] = Field(default=None, ge=1, le=65535)
    database: Optional[str] = None
    username: Optional[str] = None
    password: Optional[SecretStr] = None
    schema_name: Optional[str] = None

    @field_validator("host", "database", "username", "password", "schema_name", mode="before")
    @classmethod
    def blank_to_none(cls, value: Any) -> Any:
        """Treat empty form fields as missing."""
        if isinstance(value, str) and not value.strip():
            return None
        return value.strip() if isinstance(value, str) and not isinstance(value, SecretStr) else value

    @field_validator("host")
    @classmethod
    def check_host(cls, value: Optional[str]) -> Optional[str]:
        if value is not None and not re.fullmatch(r"[A-Za-z0-9._\-:\[\]]{1,255}", value):
            raise ValueError("Host contains invalid characters.")
        return value

    @field_validator("database", "username")
    @classmethod
    def check_names(cls, value: Optional[str]) -> Optional[str]:
        if value is not None and (len(value) > 128 or "\x00" in value):
            raise ValueError("Value is too long or invalid.")
        return value

    @field_validator("schema_name")
    @classmethod
    def check_schema(cls, value: Optional[str]) -> Optional[str]:
        if value is not None and not re.fullmatch(r"\w{1,63}", value):
            raise ValueError("Schema name may only contain letters, digits and underscores.")
        return value


class DatabaseSession:
    """One user's isolated, read-only view of one connected database."""

    def __init__(
        self,
        dialect: DialectName,
        label: str,
        sqlite_path: Optional[Path] = None,
        request: Optional[ConnectionRequest] = None,
    ) -> None:
        self.session_id = uuid.uuid4().hex
        self.dialect: DialectName = dialect
        self.label = label
        self.sqlite_path = sqlite_path
        self._request = request  # Held privately; never serialised or logged.
        self.schema: dict[str, dict] = {}
        self.fingerprint = ""
        self.created_at = time.time()
        self.last_used = time.time()
        self.lock = asyncio.Lock()
        if request is not None and request.password is not None:
            register_secret(request.password.get_secret_value())

    # -- connections --------------------------------------------------------

    @property
    def schema_name(self) -> Optional[str]:
        return self._request.schema_name if self._request else None

    def open_connection(self, timeout_seconds: int = QUERY_TIMEOUT_SECONDS) -> Any:
        """Open a fresh read-only DBAPI connection with a statement timeout."""
        if self.dialect == "sqlite":
            assert self.sqlite_path is not None
            uri = self.sqlite_path.resolve().as_uri() + "?mode=ro"
            connection = sqlite3.connect(uri, uri=True, timeout=5, check_same_thread=False)
            connection.execute("PRAGMA query_only = ON")
            connection.execute("PRAGMA trusted_schema = OFF")
            return connection

        request = self._request
        assert request is not None
        password = request.password.get_secret_value() if request.password else None

        if self.dialect == "postgresql":
            import psycopg

            options = (
                f"-c default_transaction_read_only=on "
                f"-c statement_timeout={timeout_seconds * 1000}"
            )
            if request.schema_name:
                options += f" -c search_path={request.schema_name}"
            connection = psycopg.connect(
                host=request.host, port=request.port or 5432, dbname=request.database,
                user=request.username, password=password, connect_timeout=10,
                options=options, application_name="verisql",
            )
            connection.read_only = True
            return connection

        import pymysql

        connection = pymysql.connect(
            host=request.host, port=request.port or 3306, user=request.username,
            password=password or "", database=request.database, connect_timeout=10,
            read_timeout=timeout_seconds + 5, charset="utf8mb4", autocommit=False,
        )
        cursor = connection.cursor()
        cursor.execute("SET SESSION TRANSACTION READ ONLY")
        # MySQL and MariaDB name their execution-time limits differently.
        for statement in (
            f"SET SESSION MAX_EXECUTION_TIME = {timeout_seconds * 1000}",
            f"SET SESSION max_statement_time = {timeout_seconds}",
        ):
            try:
                cursor.execute(statement)
                break
            except Exception:  # noqa: BLE001 - variable absent on this server
                continue
        cursor.execute("START TRANSACTION READ ONLY")
        cursor.close()
        return connection

    def run_select(self, sql: str, max_rows: int, timeout_seconds: int = QUERY_TIMEOUT_SECONDS):
        """Run a verified SELECT with row and time limits.

        Returns (columns, rows, truncated, elapsed_ms).
        """
        wrapped = f"SELECT * FROM ({sql}) AS verisql_result LIMIT {int(max_rows) + 1}"
        connection = self.open_connection(timeout_seconds)
        started = time.monotonic()
        try:
            if self.dialect == "sqlite":
                deadline = started + timeout_seconds
                connection.set_progress_handler(
                    lambda: 1 if time.monotonic() > deadline else 0, 10_000
                )
            cursor = connection.cursor()
            cursor.execute(wrapped)
            columns = [item[0] for item in cursor.description or []]
            rows = cursor.fetchmany(int(max_rows) + 1)
            truncated = len(rows) > max_rows
            return columns, rows[:max_rows], truncated, int((time.monotonic() - started) * 1000)
        finally:
            try:
                connection.rollback()
            except Exception:  # noqa: BLE001
                pass
            connection.close()

    def explain(self, sql: str) -> None:
        """Ask the database engine to plan the query without running it."""
        connection = self.open_connection()
        try:
            connection.cursor().execute("EXPLAIN " + sql)
        finally:
            try:
                connection.rollback()
            except Exception:  # noqa: BLE001
                pass
            connection.close()

    # -- schema -------------------------------------------------------------

    def connect_and_introspect(self) -> None:
        """Validate the connection, read the schema and fingerprint the database."""
        from sqlalchemy import create_engine, inspect
        from sqlalchemy.pool import NullPool

        connection = self.open_connection()
        try:
            connection.cursor().execute("SELECT 1")
        finally:
            connection.close()

        url = {"sqlite": "sqlite://", "postgresql": "postgresql+psycopg://", "mysql": "mysql+pymysql://"}
        engine = create_engine(url[self.dialect], creator=self.open_connection, poolclass=NullPool)
        try:
            inspector = inspect(engine)
            schema_arg = self.schema_name if self.dialect == "postgresql" else None
            result: dict[str, dict] = {}
            for table in sorted(inspector.get_table_names(schema=schema_arg)):
                if table.lower().startswith("sqlite_"):
                    continue
                primary = set(inspector.get_pk_constraint(table, schema=schema_arg).get("constrained_columns") or [])
                columns = []
                for column in inspector.get_columns(table, schema=schema_arg):
                    try:
                        type_name = str(column["type"])
                    except Exception:  # noqa: BLE001 - unsupported column type
                        type_name = "UNKNOWN"
                    columns.append({
                        "name": column["name"], "type": type_name,
                        "primary_key": column["name"] in primary,
                        "nullable": bool(column.get("nullable", True)),
                        "sensitive": is_sensitive_column(column["name"]),
                    })
                foreign_keys = []
                for fk in inspector.get_foreign_keys(table, schema=schema_arg):
                    for local, remote in zip(fk["constrained_columns"], fk["referred_columns"]):
                        foreign_keys.append({
                            "column": local, "referenced_table": fk["referred_table"],
                            "referenced_column": remote,
                        })
                result[table] = {"columns": columns, "foreign_keys": foreign_keys}
        finally:
            engine.dispose()

        if not result:
            raise ValueError("The database was reached but contains no tables.")
        if any(not details["columns"] for details in result.values()):
            raise ValueError("Schema verification failed: a table has no readable columns.")
        known = {name.lower() for name in result}
        for details in result.values():
            for fk in details["foreign_keys"]:
                if fk["referenced_table"].lower() not in known:
                    logger.warning("Foreign key references a table outside the schema.")
        self.schema = result
        self.fingerprint = self._compute_fingerprint()

    def _compute_fingerprint(self) -> str:
        """Identify the database by dialect, location (never credentials) and structure."""
        structure = {
            table: [(c["name"].lower(), c["type"].upper()) for c in details["columns"]]
            for table, details in sorted(self.schema.items())
        }
        location = ""
        if self._request is not None:
            location = f"{(self._request.host or '').lower()}:{self._request.port}/{self._request.database}"
        payload = json.dumps([self.dialect, location, structure], sort_keys=True)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]

    def cleanup(self) -> None:
        """Release resources: forget secrets and delete any uploaded file."""
        if self._request is not None and self._request.password is not None:
            unregister_secret(self._request.password.get_secret_value())
        if self.sqlite_path is not None:
            try:
                self.sqlite_path.unlink(missing_ok=True)
            except OSError:
                logger.warning("Could not delete temporary database file.")

    def summary(self) -> dict:
        """A credential-free description of the session."""
        return {
            "connected": True,
            "dialect": self.dialect,
            "label": self.label,
            "tables": len(self.schema),
            "columns": sum(len(t["columns"]) for t in self.schema.values()),
            "foreign_keys": sum(len(t["foreign_keys"]) for t in self.schema.values()),
            "connected_at": datetime.fromtimestamp(self.created_at, timezone.utc).isoformat(),
        }


class SessionStore:
    """Holds every active database session, keyed by an opaque session id."""

    def __init__(self) -> None:
        self._sessions: dict[str, DatabaseSession] = {}

    def add(self, session: DatabaseSession) -> None:
        self._sessions[session.session_id] = session

    def get(self, session_id: Optional[str]) -> Optional[DatabaseSession]:
        session = self._sessions.get(session_id or "")
        if session is not None:
            session.last_used = time.time()
        return session

    def remove(self, session_id: Optional[str]) -> bool:
        session = self._sessions.pop(session_id or "", None)
        if session is None:
            return False
        session.cleanup()
        return True

    def expire(self) -> int:
        """Remove idle sessions and return how many were removed."""
        cutoff = time.time() - SESSION_TTL_SECONDS
        stale = [sid for sid, s in self._sessions.items() if s.last_used < cutoff]
        for sid in stale:
            self.remove(sid)
        return len(stale)

    def clear(self) -> None:
        for sid in list(self._sessions):
            self.remove(sid)

    def __len__(self) -> int:
        return len(self._sessions)


SESSIONS = SessionStore()


# ---------------------------------------------------------------------------
# Verifier: deterministic SQL validation (ported from agents/evaluator.py)
# ---------------------------------------------------------------------------

def _parse_statements(sql: str, dialect: str) -> list:
    if not sql or not sql.strip():
        raise ValueError("The SQL query is empty.")
    if len(sql) > MAX_SQL_LENGTH:
        raise ValueError("The SQL query is too long.")
    return parse(sql, read=SQLGLOT_DIALECT[dialect])


def validate_sql_syntax(sql_query: str, dialect: str) -> dict:
    """Validate SQL syntax and ensure exactly one SELECT query is provided."""
    try:
        statements = _parse_statements(sql_query, dialect)
        if len(statements) != 1 or statements[0] is None:
            return {"valid": False, "error": "Only one SQL statement is allowed."}
        if not isinstance(statements[0], exp.Query):
            return {"valid": False, "error": "Only SELECT queries are allowed."}
        return {"valid": True, "error": None}
    except Exception as error:  # noqa: BLE001 - sqlglot raises several types
        return {"valid": False, "error": f"SQL could not be parsed: {error}"}


def validate_sql_safety(sql_query: str, dialect: str) -> dict:
    """Reject modifying operations, dangerous functions and sensitive access.

    The result carries a severity: "unsafe" stops the workflow immediately,
    while "repairable" lets the Writer try again.
    """
    def fail(message: str, severity: str) -> dict:
        return {"valid": False, "error": message, "severity": severity}

    try:
        statements = _parse_statements(sql_query, dialect)
    except Exception as error:  # noqa: BLE001
        return fail(f"SQL could not be parsed: {error}", "repairable")
    if len(statements) != 1 or statements[0] is None:
        return fail("Only one SQL statement is allowed.", "unsafe")
    query = statements[0]
    if not isinstance(query, exp.Query):
        return fail("Only read-only SELECT queries are allowed.", "unsafe")

    for node in query.walk():
        if isinstance(node, BLOCKED_NODES):
            return fail(f"Restricted SQL operation: {type(node).__name__}", "unsafe")

    for func in query.find_all(exp.Func):
        name = (func.name if isinstance(func, exp.Anonymous) else func.sql_name()).lower()
        if name in DANGEROUS_FUNCTIONS or name.startswith(("pg_", "lo_")):
            return fail(f"Restricted function '{name}' is not allowed.", "unsafe")

    for table in query.find_all(exp.Table):
        if table.db.lower() in SYSTEM_SCHEMAS or table.name.lower().startswith("sqlite_"):
            return fail("Access to system catalogues is restricted.", "unsafe")

    for column in query.find_all(exp.Column):
        if is_sensitive_column(column.name):
            return fail(f"Access to sensitive column '{column.name}' is restricted.", "unsafe")

    # Wildcard projections could expose sensitive columns the check above cannot see.
    for select in query.find_all(exp.Select):
        if select.find_ancestor(exp.Exists) is not None:
            continue
        for projection in select.expressions:
            if isinstance(projection, exp.Star) or (
                isinstance(projection, exp.Column) and isinstance(projection.this, exp.Star)
            ):
                return fail(
                    "Wildcard projection (SELECT * / table.*) is not allowed. "
                    "List the required non-sensitive columns explicitly.",
                    "repairable",
                )
    return {"valid": True, "error": None, "severity": None}


def _check_tables(query: exp.Expression, db: DatabaseSession) -> Optional[str]:
    """Check that every referenced table exists in the connected schema."""
    cte_names = {cte.alias_or_name.lower() for cte in query.find_all(exp.CTE)}
    known = {name.lower() for name in db.schema}
    for table in query.find_all(exp.Table):
        if not isinstance(table.this, exp.Identifier):
            continue  # Table-valued functions are handled by the safety check.
        name = table.name.lower()
        if name in cte_names:
            continue
        if table.db and db.dialect == "postgresql" and table.db.lower() != (db.schema_name or "public").lower():
            return f"Unknown schema '{table.db}'."
        if name not in known:
            return f"Unknown table '{table.name}'. Available tables: {', '.join(sorted(db.schema))}."
    return None


def validate_sql_schema(sql_query: str, db: DatabaseSession) -> dict:
    """Check referenced tables and columns against the connected database."""
    syntax = validate_sql_syntax(sql_query, db.dialect)
    if not syntax["valid"]:
        return syntax
    try:
        query = _parse_statements(sql_query, db.dialect)[0]
        problem = _check_tables(query, db)
        if problem:
            return {"valid": False, "error": problem}
        db.explain(query.sql(dialect=SQLGLOT_DIALECT[db.dialect]))
        return {"valid": True, "error": None}
    except Exception as error:  # noqa: BLE001 - driver-specific error types
        return {"valid": False, "error": scrub(error)}


def validate_sql_execution(sql_query: str, db: DatabaseSession, max_rows: int = 20) -> dict:
    """Re-validate and execute a query on a read-only connection.

    Validation is repeated here deliberately: nothing is executed on the
    strength of an earlier check alone.
    """
    empty = {"valid": False, "columns": [], "rows": [], "truncated": False, "elapsed_ms": 0}
    syntax = validate_sql_syntax(sql_query, db.dialect)
    if not syntax["valid"]:
        return {**empty, "error": syntax["error"]}
    safety = validate_sql_safety(sql_query, db.dialect)
    if not safety["valid"]:
        return {**empty, "error": safety["error"]}
    try:
        query = _parse_statements(sql_query, db.dialect)[0]
        problem = _check_tables(query, db)
        if problem:
            return {**empty, "error": problem}
        clean_sql = query.sql(dialect=SQLGLOT_DIALECT[db.dialect])
        columns, rows, truncated, elapsed = db.run_select(clean_sql, max_rows)
        return {
            "valid": True, "error": None, "columns": columns,
            "rows": to_jsonable([list(row) for row in rows]),
            "truncated": truncated, "elapsed_ms": elapsed,
        }
    except Exception as error:  # noqa: BLE001
        return {**empty, "error": scrub(error)}


# ---------------------------------------------------------------------------
# MCP servers (hosted in-process, bound to one database session)
# ---------------------------------------------------------------------------

@asynccontextmanager
async def mcp_client(server: FastMCP) -> AsyncIterator[Any]:
    """Connect a client to an in-process MCP server."""
    async with create_connected_server_and_client_session(server) as client:
        yield client


async def call_tool(server: FastMCP, name: str, arguments: dict) -> Any:
    """Call one MCP tool and decode its JSON result."""
    async with mcp_client(server) as client:
        result = await client.call_tool(name, arguments)
    text = result.content[0].text if result.content else ""
    if result.isError:
        raise RuntimeError(scrub(text) or f"MCP tool '{name}' failed.")
    return json.loads(text) if text else None


def _normalise_terms(value: Any) -> set[str]:
    words = "".join(c.lower() if c.isalnum() else " " for c in str(value)).split()
    return {word[:-1] if len(word) > 3 and word.endswith("s") else word for word in words}


def _qualified_table(db: DatabaseSession, table: str) -> str:
    quoted = quote_identifier(table, db.dialect)
    if db.dialect == "postgresql" and db.schema_name:
        return f"{quote_identifier(db.schema_name, db.dialect)}.{quoted}"
    return quoted


def build_schema_server(db: DatabaseSession) -> FastMCP:
    """Schema MCP: schema, sample values and relevant-column search."""
    server = FastMCP("VeriSQL-Schema-MCP")

    @server.tool()
    async def list_databases() -> str:
        """List the database authorised for this session."""
        return json.dumps([db.label])

    @server.tool()
    async def get_schema(tables: Optional[list[str]] = None) -> str:
        """Retrieve table columns, primary keys and foreign keys."""
        available = set(db.schema)
        selected = sorted(available if tables is None else set(tables))
        if not set(selected).issubset(available):
            raise ValueError("Unknown table requested.")
        return json.dumps({name: db.schema[name] for name in selected})

    @server.tool()
    async def sample_values(table: str, column: str, n: int = 5) -> str:
        """Retrieve limited distinct values from non-sensitive columns."""
        if not 1 <= n <= 10:
            raise ValueError("n must be between 1 and 10.")
        if table not in db.schema:
            raise ValueError("Unknown table.")
        if column not in {c["name"] for c in db.schema[table]["columns"]}:
            raise ValueError("Unknown column.")
        if is_sensitive_column(column):
            raise ValueError("Sensitive column samples are restricted.")
        sql = (
            f"SELECT DISTINCT {quote_identifier(column, db.dialect)} FROM {_qualified_table(db, table)} "
            f"WHERE {quote_identifier(column, db.dialect)} IS NOT NULL LIMIT {int(n)}"
        )

        def run() -> list:
            connection = db.open_connection(5)
            try:
                cursor = connection.cursor()
                cursor.execute(sql)
                return [row[0] for row in cursor.fetchall()]
            finally:
                connection.close()

        return json.dumps(to_jsonable(await asyncio.to_thread(run)))

    @server.tool()
    async def find_relevant_columns(question: str, k: int = 10) -> str:
        """Find relevant columns using names, table context and database values."""
        if not question.strip() or len(question) > 1000:
            raise ValueError("Invalid question.")
        if not 1 <= k <= 30:
            raise ValueError("k must be between 1 and 30.")

        def run() -> list[dict]:
            terms = _normalise_terms(question)
            matches: list[dict] = []
            scan_values = sum(len(t["columns"]) for t in db.schema.values()) <= 150
            deadline = time.monotonic() + 8
            connection = db.open_connection(3) if scan_values else None
            try:
                for table, details in db.schema.items():
                    table_score = len(_normalise_terms(table) & terms) * 2
                    for column in details["columns"]:
                        name = column["name"]
                        if column["sensitive"]:
                            continue
                        score = table_score + len(_normalise_terms(name.replace("_", " ")) & terms) * 3
                        if connection is not None and time.monotonic() < deadline:
                            query = (
                                f"SELECT DISTINCT {quote_identifier(name, db.dialect)} "
                                f"FROM {_qualified_table(db, table)} "
                                f"WHERE {quote_identifier(name, db.dialect)} IS NOT NULL LIMIT 100"
                            )
                            try:
                                cursor = connection.cursor()
                                cursor.execute(query)
                                for row in cursor.fetchall():
                                    value = _normalise_terms(row[0])
                                    if value and value.issubset(terms):
                                        score += 5
                                        break
                            except Exception:  # noqa: BLE001 - skip unreadable columns
                                try:
                                    connection.rollback()
                                except Exception:  # noqa: BLE001
                                    pass
                        if score > 2:
                            matches.append({"table": table, "column": name, "score": score})
            finally:
                if connection is not None:
                    connection.close()
            return sorted(matches, key=lambda m: (-m["score"], m["table"], m["column"]))[:k]

        return json.dumps(await asyncio.to_thread(run))

    return server


def build_verifier_server(db: DatabaseSession) -> FastMCP:
    """Verifier MCP: the four deterministic checks used by the Evaluator."""
    server = FastMCP("VeriSQL-Verifier")

    @server.tool()
    async def check_sql_syntax(sql_query: str) -> str:
        """Check SQL syntax and statement count."""
        return json.dumps(validate_sql_syntax(sql_query, db.dialect))

    @server.tool()
    async def check_sql_safety(sql_query: str) -> str:
        """Check read-only restrictions, wildcards and sensitive column access."""
        return json.dumps(validate_sql_safety(sql_query, db.dialect))

    @server.tool()
    async def check_sql_schema(sql_query: str) -> str:
        """Check referenced database tables and columns."""
        return json.dumps(await asyncio.to_thread(validate_sql_schema, sql_query, db))

    @server.tool()
    async def execute_sql_readonly(sql_query: str, max_rows: int = 20) -> str:
        """Execute a permitted SQL query using a read-only connection."""
        limit = max(1, min(int(max_rows), MAX_RESULT_ROWS))
        return json.dumps(await asyncio.to_thread(validate_sql_execution, sql_query, db, limit))

    return server


# ---------------------------------------------------------------------------
# Writer Agent (ported from agents/writer.py)
# ---------------------------------------------------------------------------

class SQLGeneration(BaseModel):
    """Structured output of the Writer Agent."""

    sql_query: str = Field(description="Generated SELECT query for the connected database dialect")
    explanation: str = Field(description="Brief explanation of the SQL query")
    tables_used: list[str] = Field(default_factory=list, description="Tables referenced in the query")
    assumptions: list[str] = Field(default_factory=list, description="Assumptions made while generating SQL")


class EvaluationResult(BaseModel):
    """Structured output of the Evaluator Agent (ported from the notebook)."""

    syntax_valid: bool
    schema_valid: bool
    safety_valid: bool
    execution_success: bool
    semantic_correct: bool
    decision: Literal["accept", "reject"]
    errors: list[str] = Field(default_factory=list)
    feedback: str = Field(default="", description="Actionable repair instructions for the Writer Agent")
    confidence: float = Field(default=0.5, ge=0.0, le=1.0)


_sql_parser = PydanticOutputParser(pydantic_object=SQLGeneration)
_evaluation_parser = PydanticOutputParser(pydantic_object=EvaluationResult)

WRITER_PROMPT = ChatPromptTemplate.from_messages([
    ("system", """You are the Writer Agent in VeriSQL.
Generate accurate {dialect_name} SELECT queries using only the supplied database schema.
Use existing tables and columns, follow foreign keys for JOINs, never invent schema details,
never access sensitive columns, never use SELECT * or table.* (list the columns explicitly),
and generate only read-only SELECT queries. State assumptions when the question is ambiguous.
{format_instructions}"""),
    ("human", """User question: {question}
Previous experiences (reference only, never copy blindly):
{memory_context}
Relevant columns: {relevant_columns}
Sample values: {sample_values}
Complete database schema: {schema}
Generate the SQL query."""),
])

REPAIR_PROMPT = ChatPromptTemplate.from_messages([
    ("system", """You are the VeriSQL Writer Agent.
Repair the previous SQL using the evaluator's feedback.
Use only the supplied {dialect_name} schema. Generate one read-only SELECT query
with explicit columns (no SELECT * or table.*). Do not repeat the identified mistake.
{format_instructions}"""),
    ("human", """Original question: {question}
Previous SQL: {previous_sql}
Evaluator feedback: {evaluator_feedback}
Relevant columns: {relevant_columns}
Sample values: {sample_values}
Database schema: {schema}
Generate corrected SQL."""),
])

EVALUATOR_PROMPT = ChatPromptTemplate.from_messages([
    ("system", """You are an independent SQL Evaluation Agent in VeriSQL.
Your task is to determine whether generated {dialect_name} SQL correctly answers the
user's natural language question.

ACTUAL DATABASE SCHEMA:
{database_schema}

Follow these evaluation rules:
1. Check whether the SQL correctly answers the user's question.
2. Identify missing filters, joins, aggregations and conditions.
3. Use only tables and columns present in the supplied schema.
4. Follow the actual foreign-key relationships when suggesting JOINs.
5. Never invent table names, column names or relationships.
6. Do not confuse successful execution with semantic correctness.
7. Treat the Verifier's deterministic results as authoritative.
8. Reject SQL if any deterministic validation fails.
9. If the query is incorrect, provide specific repair instructions.
10. Do not approve SQL when semantic correctness is uncertain.

The database schema is authoritative. Do not use assumptions that contradict it.
{format_instructions}"""),
    ("human", """USER QUESTION:
{question}

GENERATED SQL:
{sql_query}

VERIFIER VALIDATION REPORT:
{validation_report}

SQL EXECUTION RESULTS (preview):
{execution_results}

Evaluate whether the SQL correctly answers the question."""),
])

# Tests may replace this with a factory returning a fake chat model.
_LLM_FACTORY: Optional[Any] = None
_CHAIN_CACHE: dict[str, Any] = {}


def llm_configured() -> bool:
    return bool(GROQ_API_KEY) or _LLM_FACTORY is not None


def get_chain(name: str) -> Any:
    """Build (once) and return the writer, repair or evaluator chain."""
    if name in _CHAIN_CACHE:
        return _CHAIN_CACHE[name]
    if _LLM_FACTORY is not None:
        llm = _LLM_FACTORY(name)
    else:
        if not GROQ_API_KEY:
            raise LLMServiceError("GROQ_API_KEY is not configured.")
        from langchain_groq import ChatGroq

        model = EVALUATOR_MODEL if name == "evaluator" else WRITER_MODEL
        llm = ChatGroq(model=model, temperature=0, api_key=GROQ_API_KEY,
                       timeout=LLM_TIMEOUT_SECONDS, max_retries=2)
    prompt, parser = {
        "writer": (WRITER_PROMPT, _sql_parser),
        "repair": (REPAIR_PROMPT, _sql_parser),
        "evaluator": (EVALUATOR_PROMPT, _evaluation_parser),
    }[name]
    _CHAIN_CACHE[name] = prompt | llm | parser
    return _CHAIN_CACHE[name]


async def get_database_context(db: DatabaseSession, question: str) -> dict:
    """Gather the schema context for the Writer through the Schema MCP."""
    schema_server = build_schema_server(db)
    relevant = await call_tool(schema_server, "find_relevant_columns", {"question": question})
    tables: Optional[list[str]] = None
    if len(db.schema) > MAX_CONTEXT_TABLES:
        wanted = {item["table"] for item in relevant}
        for table in list(wanted):
            for fk in db.schema[table]["foreign_keys"]:
                wanted.add(fk["referenced_table"])
        tables = sorted(t for t in wanted if t in db.schema)[:MAX_CONTEXT_TABLES] or sorted(db.schema)[:MAX_CONTEXT_TABLES]
    schema = await call_tool(schema_server, "get_schema", {"tables": tables})
    samples: dict[str, list] = {}
    for item in relevant[:5]:
        try:
            values = await call_tool(
                schema_server, "sample_values", {"table": item["table"], "column": item["column"], "n": 5}
            )
            samples[f"{item['table']}.{item['column']}"] = values
        except Exception:  # noqa: BLE001 - samples are optional context
            continue
    return {
        "relevant_columns": json.dumps(relevant),
        "schema": json.dumps(schema, separators=(",", ":")),
        "sample_values": json.dumps(samples, default=str),
    }


def build_writer_server(db: DatabaseSession) -> FastMCP:
    """Writer MCP: SQL generation and repair."""
    server = FastMCP("VeriSQL Writer MCP")
    base = {
        "dialect_name": DIALECT_LABEL[db.dialect],
        "format_instructions": _sql_parser.get_format_instructions(),
    }

    @server.tool()
    async def generate_sql(question: str, memory_context: str = "") -> str:
        """Generate SQL from a natural-language question."""
        context = await get_database_context(db, question)
        result = await get_chain("writer").ainvoke({
            **base, **context, "question": question,
            "memory_context": memory_context or "No relevant previous experiences available.",
        })
        return result.model_dump_json()

    @server.tool()
    async def repair_sql(question: str, previous_sql: str, evaluator_feedback: str) -> str:
        """Repair SQL using Evaluator Agent feedback."""
        context = await get_database_context(db, question)
        result = await get_chain("repair").ainvoke({
            **base, **context, "question": question,
            "previous_sql": previous_sql, "evaluator_feedback": evaluator_feedback,
        })
        return result.model_dump_json()

    return server


# ---------------------------------------------------------------------------
# Evaluator Agent (deterministic checks via Verifier MCP, then LLM semantics)
# ---------------------------------------------------------------------------

def validate_evaluator_output(evaluation: EvaluationResult, all_checks_passed: bool) -> EvaluationResult:
    """Enforce consistency in the Evaluator's structured response."""
    if not all_checks_passed or not evaluation.semantic_correct:
        evaluation.decision = "reject"
    if not all_checks_passed:
        evaluation.semantic_correct = False
    if evaluation.decision == "reject" and not evaluation.feedback.strip():
        evaluation.feedback = (
            "Correct the following issues: " + "; ".join(evaluation.errors)
            if evaluation.errors
            else "Re-evaluate the SQL against the original question and database schema."
        )
    evaluation.confidence = max(0.0, min(1.0, evaluation.confidence))
    return evaluation


async def evaluate_sql(db: DatabaseSession, question: str, sql_query: str) -> dict:
    """Evaluate SQL with the Verifier MCP and a fail-closed semantic review."""
    verifier = build_verifier_server(db)
    not_run = {"valid": False, "error": "Not run because an earlier check failed."}
    async with mcp_client(verifier) as client:
        async def run(tool: str, **extra: Any) -> dict:
            result = await client.call_tool(tool, {"sql_query": sql_query, **extra})
            if result.isError:
                return {"valid": False, "error": "Verifier tool failed."}
            return json.loads(result.content[0].text)

        syntax = await run("check_sql_syntax")
        safety = await run("check_sql_safety")
        schema = not_run
        execution = {**not_run, "columns": [], "rows": []}
        if syntax["valid"] and safety["valid"]:
            schema = await run("check_sql_schema")
            if schema["valid"]:
                execution = await run("execute_sql_readonly", max_rows=20)

    checks = [("Syntax", syntax), ("Safety", safety), ("Schema", schema), ("Execution", execution)]
    errors = [f"{name}: {item['error']}" for name, item in checks if not item["valid"]]
    report = {
        "syntax": syntax, "safety": safety, "schema": schema,
        "execution": {"valid": execution["valid"], "error": execution.get("error")},
    }

    def package(result: EvaluationResult) -> dict:
        data = result.model_dump()
        data.update({
            "accepted": data["decision"] == "accept", "syntax": syntax, "safety": safety,
            "schema": schema, "execution": report["execution"],
        })
        return data

    if errors:
        # Deterministic failures bypass the LLM entirely.
        first = next(item["error"] for _, item in checks if not item["valid"])
        return package(EvaluationResult(
            syntax_valid=syntax["valid"], schema_valid=schema["valid"], safety_valid=safety["valid"],
            execution_success=execution["valid"], semantic_correct=False, decision="reject",
            errors=errors, feedback=first, confidence=1.0,
        ))

    preview = {
        "columns": execution["columns"],
        "rows": execution["rows"][:EVALUATOR_PREVIEW_ROWS],
        "rows_returned": len(execution["rows"]),
    }
    try:
        evaluation = await get_chain("evaluator").ainvoke({
            "dialect_name": DIALECT_LABEL[db.dialect], "question": question, "sql_query": sql_query,
            "database_schema": json.dumps(db.schema, separators=(",", ":")),
            "validation_report": json.dumps(report, indent=2, default=str),
            "execution_results": json.dumps(preview, indent=2, default=str),
            "format_instructions": _evaluation_parser.get_format_instructions(),
        })
    except Exception as error:  # noqa: BLE001 - fail closed on any LLM failure
        logger.warning("Semantic evaluation failed: %s", type(error).__name__)
        return package(EvaluationResult(
            syntax_valid=True, schema_valid=True, safety_valid=True, execution_success=True,
            semantic_correct=False, decision="reject",
            errors=["Semantic evaluation could not be completed."],
            feedback="Semantic evaluation could not be completed. Simplify the query and make it exactly match the question.",
            confidence=0.0,
        ))
    evaluation.syntax_valid = evaluation.schema_valid = evaluation.safety_valid = True
    evaluation.execution_success = True
    return package(validate_evaluator_output(evaluation, all_checks_passed=True))


# ---------------------------------------------------------------------------
# Long-term memory (SQLite + MiniLM embeddings, scoped to one database)
# ---------------------------------------------------------------------------

class EmbeddingService:
    """Lazily loads the sentence-transformers model and encodes text."""

    def __init__(self) -> None:
        self._model: Any = None
        self._lock = threading.Lock()

    @property
    def loaded(self) -> bool:
        return self._model is not None

    def warm_up(self) -> None:
        with self._lock:
            if self._model is None:
                from sentence_transformers import SentenceTransformer

                self._model = SentenceTransformer(EMBEDDING_MODEL_NAME)

    def encode(self, text: str) -> np.ndarray:
        self.warm_up()
        vector = self._model.encode(text, normalize_embeddings=True).astype(np.float32)
        if vector.shape != (EMBEDDING_DIMENSION,):
            raise ValueError("Unexpected embedding dimension.")
        return vector


EMBEDDER = EmbeddingService()


def normalise_outcome(outcome: str) -> str:
    """Map the notebook's mixed labels ('accept'/'accepted') to one vocabulary."""
    lowered = (outcome or "").lower()
    if lowered in {"accept", "accepted"}:
        return "accepted"
    if lowered == "unsafe":
        return "unsafe"
    return "rejected"


class MemoryStore:
    """Persistent, database-aware semantic memory."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=10)
        connection.row_factory = sqlite3.Row
        return connection

    def init(self) -> None:
        """Create the table, or migrate the notebook's table in place."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.execute("""
                CREATE TABLE IF NOT EXISTS agent_memories (
                    memory_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT, question TEXT NOT NULL, sql_query TEXT NOT NULL,
                    outcome TEXT NOT NULL, feedback TEXT, embedding BLOB NOT NULL,
                    metadata TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
            existing = {row["name"] for row in connection.execute("PRAGMA table_info(agent_memories)")}
            for column in ("db_fingerprint", "db_label", "repair_history"):
                if column not in existing:
                    connection.execute(f"ALTER TABLE agent_memories ADD COLUMN {column} TEXT")
            connection.execute("CREATE INDEX IF NOT EXISTS idx_memory_outcome ON agent_memories(outcome)")
            connection.execute("CREATE INDEX IF NOT EXISTS idx_memory_db ON agent_memories(db_fingerprint)")

    def store(self, db: DatabaseSession, question: str, sql_query: str, outcome: str,
              feedback: str, repair_history: list, metadata: dict) -> bool:
        """Store one experience. Returns False when embedding is unavailable."""
        try:
            embedding = EMBEDDER.encode(question)
        except Exception as error:  # noqa: BLE001 - memory must never block the workflow
            logger.warning("Memory not stored (embedding unavailable): %s", type(error).__name__)
            return False
        with self._connect() as connection:
            connection.execute(
                """INSERT INTO agent_memories (session_id, question, sql_query, outcome, feedback,
                   embedding, metadata, db_fingerprint, db_label, repair_history)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (db.session_id, question, sql_query, outcome, feedback, embedding.tobytes(),
                 json.dumps(metadata), db.fingerprint, db.label, json.dumps(repair_history)),
            )
        return True

    def retrieve(self, db: DatabaseSession, question: str, top_k: int = 3,
                 min_similarity: float = 0.35) -> tuple[list[dict], bool]:
        """Return similar experiences from the same database only."""
        try:
            query = EMBEDDER.encode(question)
        except Exception as error:  # noqa: BLE001
            logger.warning("Memory retrieval skipped (embedding unavailable): %s", type(error).__name__)
            return [], False
        with self._connect() as connection:
            records = connection.execute(
                "SELECT * FROM agent_memories WHERE db_fingerprint = ?", (db.fingerprint,)
            ).fetchall()
        memories = []
        for record in records:
            stored = np.frombuffer(record["embedding"], dtype=np.float32)
            if stored.shape != query.shape:
                continue
            similarity = float(np.dot(query, stored))
            if similarity >= min_similarity:
                memories.append({
                    "memory_id": record["memory_id"], "question": record["question"],
                    "sql_query": record["sql_query"], "outcome": normalise_outcome(record["outcome"]),
                    "feedback": record["feedback"] or "", "similarity": round(similarity, 4),
                    "created_at": record["created_at"],
                })
        # Successful experiences first, then by similarity (as in the notebook).
        memories.sort(key=lambda m: (m["outcome"] == "accepted", m["similarity"]), reverse=True)
        return memories[:top_k], True

    def history(self, db: DatabaseSession, limit: int = 50) -> list[dict]:
        with self._connect() as connection:
            records = connection.execute(
                """SELECT memory_id, question, sql_query, outcome, feedback, repair_history,
                          metadata, created_at FROM agent_memories
                   WHERE db_fingerprint = ? ORDER BY memory_id DESC LIMIT ?""",
                (db.fingerprint, limit),
            ).fetchall()
        items = []
        for record in records:
            metadata = json.loads(record["metadata"] or "{}")
            items.append({
                "id": record["memory_id"], "question": record["question"],
                "sql": record["sql_query"], "outcome": normalise_outcome(record["outcome"]),
                "feedback": record["feedback"] or "",
                "repair_count": metadata.get("repair_count", 0),
                "repair_history": json.loads(record["repair_history"] or "[]"),
                "created_at": record["created_at"],
            })
        return items


MEMORY = MemoryStore(MEMORY_DB_PATH)


def build_memory_context(memories: list[dict]) -> str:
    """Format retrieved experiences for the Writer (ported from the notebook)."""
    if not memories:
        return "No relevant previous experiences available."
    accepted = [m for m in memories if m["outcome"] == "accepted"]
    others = [m for m in memories if m["outcome"] != "accepted"]
    sections: list[str] = []
    if accepted:
        sections.append("PREVIOUS SUCCESSFUL EXPERIENCES:")
        sections += [f"Question: {m['question']}\nSQL pattern: {m['sql_query']}\nFeedback: {m['feedback']}" for m in accepted]
    if others:
        sections.append("\nPREVIOUS FAILED EXPERIENCES - USE AS WARNINGS:")
        sections += [f"Question: {m['question']}\nFailed SQL: {m['sql_query']}\nFailure feedback: {m['feedback']}" for m in others]
    return "\n\n".join(sections)


# ---------------------------------------------------------------------------
# LangGraph workflow (same topology as the notebook, plus a fresh-validation
# execution step as required for read-only result delivery)
# ---------------------------------------------------------------------------

class VeriSQLState(TypedDict, total=False):
    session_id: str
    question: str
    retrieved_memories: list[dict]
    memory_context: str
    memory_available: bool
    writer_response: dict
    sql_query: str
    evaluation: dict
    execution_results: dict
    feedback: str
    repair_count: int
    attempts: list[dict]
    final_status: str
    final_response: str
    workflow_history: list[dict]


def _db_for(state: VeriSQLState) -> DatabaseSession:
    db = SESSIONS.get(state["session_id"])
    if db is None:
        raise ApiError(409, "no_database", "The database session ended during the request.")
    return db


def _history(state: VeriSQLState, entry: dict) -> list[dict]:
    return state.get("workflow_history", []) + [entry]


async def _writer_call(db: DatabaseSession, tool: str, arguments: dict) -> dict:
    try:
        return await call_tool(build_writer_server(db), tool, arguments)
    except Exception as error:  # noqa: BLE001
        logger.error("Writer agent failed: %s: %s", type(error).__name__, scrub(error))
        raise LLMServiceError("The Writer Agent could not produce a valid response.") from error


async def memory_retrieval_node(state: VeriSQLState) -> dict:
    db = _db_for(state)
    memories, available = await asyncio.to_thread(MEMORY.retrieve, db, state["question"])
    return {
        "retrieved_memories": memories, "memory_available": available,
        "memory_context": build_memory_context(memories),
        "workflow_history": _history(state, {"step": "memory_retrieval", "memories_retrieved": len(memories)}),
    }


async def writer_node(state: VeriSQLState) -> dict:
    response = await _writer_call(_db_for(state), "generate_sql", {
        "question": state["question"], "memory_context": state.get("memory_context", ""),
    })
    return {"writer_response": response, "workflow_history": _history(state, {"step": "writer_agent"})}


async def evaluator_node(state: VeriSQLState) -> dict:
    sql_query = state["writer_response"]["sql_query"].strip()
    evaluation = await evaluate_sql(_db_for(state), state["question"], sql_query)
    attempt = {
        "attempt": len(state.get("attempts", [])), "sql": sql_query,
        "decision": evaluation["decision"], "feedback": evaluation.get("feedback", ""),
        "checks": {name: evaluation[name]["valid"] for name in ("syntax", "safety", "schema", "execution")},
    }
    return {
        "sql_query": sql_query, "evaluation": evaluation, "feedback": evaluation.get("feedback", ""),
        "attempts": state.get("attempts", []) + [attempt],
        "workflow_history": _history(state, {"step": "evaluator_agent", "decision": evaluation["decision"]}),
    }


def route_after_evaluation(state: VeriSQLState) -> Literal["accept", "repair", "stop"]:
    evaluation = state.get("evaluation", {})
    if evaluation.get("decision") == "accept":
        return "accept"
    if evaluation.get("safety", {}).get("severity") == "unsafe":
        return "stop"  # Unsafe SQL is rejected immediately, never repaired.
    if state.get("repair_count", 0) >= MAX_REPAIR_ATTEMPTS:
        return "stop"
    return "repair"


async def repair_node(state: VeriSQLState) -> dict:
    response = await _writer_call(_db_for(state), "repair_sql", {
        "question": state["question"], "previous_sql": state["sql_query"],
        "evaluator_feedback": state["feedback"],
    })
    count = state.get("repair_count", 0) + 1
    return {
        "writer_response": response, "repair_count": count,
        "workflow_history": _history(state, {"step": "sql_repair", "repair_attempt": count}),
    }


async def execute_node(state: VeriSQLState) -> dict:
    """Run the accepted SQL, with a fresh validation, on the read-only connection."""
    db = _db_for(state)
    result = await call_tool(build_verifier_server(db), "execute_sql_readonly", {
        "sql_query": state["sql_query"], "max_rows": MAX_RESULT_ROWS,
    })
    return {"execution_results": result,
            "workflow_history": _history(state, {"step": "execution", "success": result["valid"]})}


def final_response_node(state: VeriSQLState) -> dict:
    evaluation = state.get("evaluation", {})
    execution = state.get("execution_results", {})
    if evaluation.get("decision") == "accept" and execution.get("valid"):
        status, message = "accepted", "SQL query generated, verified and executed successfully."
    elif evaluation.get("decision") == "accept":
        status = "execution_failed"
        message = f"SQL was verified but execution failed: {execution.get('error', 'unknown error')}"
    elif evaluation.get("safety", {}).get("severity") == "unsafe":
        status, message = "unsafe", f"Query rejected for safety reasons: {state.get('feedback', '')}"
    else:
        status = "rejected"
        message = f"SQL query could not be verified: {state.get('feedback') or 'maximum repair attempts reached.'}"
    return {"final_status": status, "final_response": message,
            "workflow_history": _history(state, {"step": "final_response", "status": status})}


async def memory_storage_node(state: VeriSQLState) -> dict:
    db = _db_for(state)
    status = state.get("final_status", "rejected")
    unsafe = status == "unsafe"
    stored = await asyncio.to_thread(
        MEMORY.store, db, state["question"],
        "[SQL withheld due to safety violation]" if unsafe else state.get("sql_query", ""),
        status, state.get("feedback", ""),
        [{"attempt": a["attempt"], "sql": a["sql"] if not unsafe else "[withheld]",
          "decision": a["decision"], "feedback": a["feedback"]} for a in state.get("attempts", [])],
        {"repair_count": state.get("repair_count", 0), "database_label": db.label,
         "execution_success": bool(state.get("execution_results", {}).get("valid"))},
    )
    return {"workflow_history": _history(state, {"step": "memory_storage", "stored": stored})}


def build_workflow() -> Any:
    """Assemble and compile the LangGraph workflow (no checkpointer by design)."""
    graph = StateGraph(VeriSQLState)
    graph.add_node("memory_retrieval", memory_retrieval_node)
    graph.add_node("writer", writer_node)
    graph.add_node("evaluator", evaluator_node)
    graph.add_node("repair", repair_node)
    graph.add_node("execute", execute_node)
    graph.add_node("final_response", final_response_node)
    graph.add_node("memory_storage", memory_storage_node)
    graph.add_edge(START, "memory_retrieval")
    graph.add_edge("memory_retrieval", "writer")
    graph.add_edge("writer", "evaluator")
    graph.add_conditional_edges(
        "evaluator", route_after_evaluation,
        {"accept": "execute", "repair": "repair", "stop": "final_response"},
    )
    graph.add_edge("repair", "evaluator")
    graph.add_edge("execute", "final_response")
    graph.add_edge("final_response", "memory_storage")
    graph.add_edge("memory_storage", END)
    return graph.compile()


# ---------------------------------------------------------------------------
# API models and application
# ---------------------------------------------------------------------------

class QueryRequest(BaseModel):
    question: str = Field(min_length=3, max_length=1000)

    @field_validator("question")
    @classmethod
    def strip(cls, value: str) -> str:
        value = value.strip()
        if len(value) < 3:
            raise ValueError("Question is too short.")
        return value


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Prepare storage, the workflow and background housekeeping."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    for stale in UPLOAD_DIR.glob("*.db"):  # Leftovers from an unclean shutdown.
        stale.unlink(missing_ok=True)
    MEMORY.init()
    app.state.workflow = build_workflow()
    if not llm_configured():
        logger.warning("GROQ_API_KEY is not set: connections work, but questions cannot be answered.")

    async def housekeeping() -> None:
        while True:
            await asyncio.sleep(60)
            removed = SESSIONS.expire()
            if removed:
                logger.info("Expired %d idle database session(s).", removed)

    tasks = [asyncio.create_task(housekeeping())]
    if os.getenv("VERISQL_PRELOAD_EMBEDDINGS", "1") == "1":
        tasks.append(asyncio.create_task(asyncio.to_thread(_safe_warm_up)))
    logger.info("VeriSQL started.")
    try:
        yield
    finally:
        for task in tasks:
            task.cancel()
        SESSIONS.clear()
        logger.info("VeriSQL stopped.")


def _safe_warm_up() -> None:
    try:
        EMBEDDER.warm_up()
        logger.info("Embedding model loaded.")
    except Exception as error:  # noqa: BLE001
        logger.warning("Embedding model unavailable; long-term memory is disabled: %s", type(error).__name__)


app = FastAPI(title="VeriSQL", version="1.0.0", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=FRONTEND_DIR, check_dir=False), name="static")


@app.exception_handler(ApiError)
async def api_error_handler(_: Request, error: ApiError) -> JSONResponse:
    return JSONResponse(status_code=error.status, content={"error": {"code": error.code, "message": error.message}})


@app.exception_handler(RequestValidationError)
async def validation_error_handler(_: Request, error: RequestValidationError) -> JSONResponse:
    # Only locations and messages are returned, never the submitted values.
    details = [{"field": ".".join(str(p) for p in e["loc"] if p != "body"), "message": e["msg"]} for e in error.errors()]
    return JSONResponse(status_code=422, content={"error": {"code": "invalid_request", "message": "Invalid request.", "details": details}})


@app.exception_handler(Exception)
async def unhandled_error_handler(_: Request, error: Exception) -> JSONResponse:
    logger.error("Unhandled error: %s: %s", type(error).__name__, scrub(error))
    return JSONResponse(status_code=500, content={"error": {"code": "internal_error", "message": "An internal error occurred."}})


def require_session(request: Request) -> DatabaseSession:
    """Dependency: the caller's connected database session."""
    session = SESSIONS.get(request.cookies.get(SESSION_COOKIE))
    if session is None:
        raise ApiError(409, "no_database", "Connect a database first.")
    return session


async def _save_upload(file: UploadFile) -> Path:
    """Stream an uploaded SQLite file to a private temporary path, validating it."""
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    handle, name = tempfile.mkstemp(suffix=".db", dir=UPLOAD_DIR)
    path = Path(name)
    size = 0
    try:
        with os.fdopen(handle, "wb") as target:
            while chunk := await file.read(1024 * 1024):
                size += len(chunk)
                if size > MAX_UPLOAD_BYTES:
                    raise ApiError(413, "file_too_large", f"The file exceeds the {MAX_UPLOAD_BYTES // (1024 * 1024)} MB limit.")
                target.write(chunk)
        with open(path, "rb") as check:
            if check.read(16) != b"SQLite format 3\x00":
                raise ApiError(400, "invalid_sqlite", "The uploaded file is not a SQLite database.")
        os.chmod(path, 0o600)
        return path
    except BaseException:
        path.unlink(missing_ok=True)
        raise


@app.post("/api/database/connect")
async def connect_database(
    request: Request,
    response: Response,
    db_type: str = Form(...),
    host: Optional[str] = Form(None),
    port: Optional[int] = Form(None),
    database: Optional[str] = Form(None),
    username: Optional[str] = Form(None),
    password: Optional[str] = Form(None),
    schema_name: Optional[str] = Form(None),
    file: Optional[UploadFile] = File(None),
) -> dict:
    """Connect, validate and fingerprint a database, then open a session."""
    register_secret(password)
    try:
        details = ConnectionRequest(
            db_type=db_type, host=host, port=port, database=database,
            username=username, password=password, schema_name=schema_name,
        )
    except ValidationError as error:
        unregister_secret(password)
        messages = "; ".join(f"{'.'.join(str(p) for p in e['loc'])}: {e['msg']}" for e in error.errors())
        raise ApiError(422, "invalid_request", messages) from None

    sqlite_path: Optional[Path] = None
    try:
        if details.db_type == "sqlite":
            if file is None or not file.filename:
                raise ApiError(400, "file_required", "Upload a SQLite database file.")
            sqlite_path = await _save_upload(file)
            label = Path(file.filename).name[:100]
        else:
            if not (details.host and details.database and details.username):
                raise ApiError(400, "missing_fields", "Host, database and username are required.")
            label = f"{details.host}/{details.database}"
        session = DatabaseSession(details.db_type, label, sqlite_path, details)
        unregister_secret(password)  # The session now holds its own reference.
        try:
            await asyncio.to_thread(session.connect_and_introspect)
        except Exception as error:  # noqa: BLE001 - driver errors vary widely
            session.cleanup()
            logger.info("Connection failed (%s): %s", details.db_type, scrub(error)[:200])
            raise ApiError(400, "connection_failed", f"Could not connect: {scrub(error)[:300]}") from None
    except BaseException:
        if sqlite_path is not None:
            sqlite_path.unlink(missing_ok=True)
        if "session" not in locals():
            unregister_secret(password)
        raise

    SESSIONS.remove(request.cookies.get(SESSION_COOKIE))  # Replace any earlier connection.
    SESSIONS.add(session)
    response.set_cookie(
        SESSION_COOKIE, session.session_id, httponly=True, samesite="strict",
        secure=request.url.scheme == "https", max_age=SESSION_TTL_SECONDS,
    )
    logger.info("Connected %s database (%d tables).", session.dialect, len(session.schema))
    return session.summary()


@app.post("/api/database/disconnect")
async def disconnect_database(request: Request, response: Response) -> dict:
    """End the caller's session and release its resources."""
    SESSIONS.remove(request.cookies.get(SESSION_COOKIE))
    response.delete_cookie(SESSION_COOKIE)
    return {"connected": False}


@app.get("/api/database/status")
async def database_status(request: Request) -> dict:
    session = SESSIONS.get(request.cookies.get(SESSION_COOKIE))
    return session.summary() if session else {"connected": False}


@app.get("/api/database/schema")
async def database_schema(session: DatabaseSession = Depends(require_session)) -> dict:
    return {"dialect": session.dialect, "label": session.label, "tables": session.schema}


@app.post("/api/query")
async def run_query(body: QueryRequest, request: Request, session: DatabaseSession = Depends(require_session)) -> dict:
    """Run the agentic workflow for one natural-language question."""
    if not llm_configured():
        raise ApiError(503, "llm_unavailable", "The language model is not configured on the server.")
    started = time.monotonic()
    initial: VeriSQLState = {
        "session_id": session.session_id, "question": body.question, "retrieved_memories": [],
        "memory_context": "", "writer_response": {}, "sql_query": "", "evaluation": {},
        "execution_results": {}, "feedback": "", "repair_count": 0, "attempts": [],
        "final_response": "", "workflow_history": [],
    }
    async with session.lock:  # One question at a time per session.
        try:
            state = await asyncio.wait_for(
                request.app.state.workflow.ainvoke(initial, config={"recursion_limit": 40}),
                timeout=LLM_TIMEOUT_SECONDS * 8,
            )
        except LLMServiceError as error:
            raise ApiError(502, "llm_error", str(error)) from None
        except asyncio.TimeoutError:
            raise ApiError(504, "timeout", "The request took too long and was cancelled.") from None
    evaluation = state.get("evaluation", {})
    execution = state.get("execution_results", {}) if state.get("final_status") == "accepted" else {}
    logger.info("Query finished: status=%s repairs=%d", state.get("final_status"), state.get("repair_count", 0))
    return {
        "status": state.get("final_status"),
        "message": state.get("final_response"),
        "sql": state.get("sql_query"),
        "explanation": state.get("writer_response", {}).get("explanation", ""),
        "assumptions": state.get("writer_response", {}).get("assumptions", []),
        "validation": {
            name: {"valid": evaluation.get(name, {}).get("valid", False), "error": evaluation.get(name, {}).get("error")}
            for name in ("syntax", "safety", "schema", "execution")
        } | {"semantic_correct": evaluation.get("semantic_correct", False)},
        "repair_attempts": state.get("repair_count", 0),
        "attempts": state.get("attempts", []),
        "columns": execution.get("columns", []),
        "rows": execution.get("rows", []),
        "row_count": len(execution.get("rows", [])),
        "truncated": execution.get("truncated", False),
        "execution_ms": execution.get("elapsed_ms"),
        "memories_used": len(state.get("retrieved_memories", [])),
        "memory_available": state.get("memory_available", False),
        "total_ms": int((time.monotonic() - started) * 1000),
    }


@app.get("/api/history")
async def query_history(limit: int = 50, session: DatabaseSession = Depends(require_session)) -> dict:
    limit = max(1, min(limit, 100))
    return {"history": await asyncio.to_thread(MEMORY.history, session, limit)}


@app.get("/api/health")
async def health() -> dict:
    return {
        "status": "ok", "llm_configured": llm_configured(),
        "embedding_model_loaded": EMBEDDER.loaded, "active_sessions": len(SESSIONS),
    }


@app.get("/")
async def index() -> Response:
    page = FRONTEND_DIR / "index.html"
    if not page.is_file():
        return JSONResponse(status_code=404, content={"error": {"code": "frontend_missing", "message": "frontend/index.html was not found."}})
    return FileResponse(page)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app:app", host=os.getenv("HOST", "127.0.0.1"), port=_env_int("PORT", 8000))
