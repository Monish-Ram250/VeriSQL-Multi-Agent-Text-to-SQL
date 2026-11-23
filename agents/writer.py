
import os
import json
import sqlite3
from pathlib import Path

from dotenv import load_dotenv
from pydantic import BaseModel, Field
from langchain_groq import ChatGroq
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import PydanticOutputParser
from sqlglot import parse, exp
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
SCHEMA_SERVER_PATH = PROJECT_ROOT / "mcp_servers" / "schema" / "server.py"
REPAIR_LOG_PATH = DATA_DIR / "writer_repair_logs.jsonl"

load_dotenv(PROJECT_ROOT / ".env")

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
if not GROQ_API_KEY:
    raise EnvironmentError("GROQ_API_KEY missing from .env")

llm = ChatGroq(model="openai/gpt-oss-20b", temperature=0, api_key=GROQ_API_KEY)

class SQLGeneration(BaseModel):
    sql_query: str = Field(description="Generated SQLite SELECT query")
    explanation: str = Field(description="Brief explanation of the SQL query")
    tables_used: list[str] = Field(description="Tables referenced in the query")
    assumptions: list[str] = Field(description="Assumptions made while generating SQL")

sql_parser = PydanticOutputParser(pydantic_object=SQLGeneration)
format_instructions = sql_parser.get_format_instructions()

async def get_database_context(question: str, db_id: str = "verisql_demo"):
    server_params = StdioServerParameters(command=os.sys.executable, args=[str(SCHEMA_SERVER_PATH)])
    with open(PROJECT_ROOT / "schema_mcp_errors.log", "w", encoding="utf-8") as server_log:
        async with stdio_client(server_params, errlog=server_log) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                relevant = await session.call_tool("find_relevant_columns", {"db_id": db_id, "question": question})
                schema = await session.call_tool("get_schema", {"db_id": db_id})
                return {
                    "relevant_columns": [item.text for item in relevant.content],
                    "schema": schema.content[0].text
                }

writer_prompt = ChatPromptTemplate.from_messages([
    ("system", """You are the Writer Agent in VeriSQL.
Generate accurate SQLite SELECT queries using only the supplied database schema.
Use existing tables and columns, follow foreign keys for JOINs, never invent schema details,
never access sensitive columns, and generate only read-only SELECT queries.
State assumptions when the question is ambiguous.
{format_instructions}"""),
    ("human", """User question: {question}
Relevant columns: {relevant_columns}
Complete database schema: {schema}
Generate the SQL query.""")
])

writer_chain = writer_prompt | llm | sql_parser

async def generate_sql(question: str, db_id: str = "verisql_demo") -> SQLGeneration:
    context = await get_database_context(question, db_id)
    return writer_chain.invoke({
        "question": question,
        "relevant_columns": "\n".join(context["relevant_columns"]),
        "schema": context["schema"],
        "format_instructions": format_instructions
    })

async def repair_sql(question: str, previous_sql: str, evaluator_feedback: str, db_id: str = "verisql_demo") -> SQLGeneration:
    context = await get_database_context(question, db_id)
    repair_prompt = ChatPromptTemplate.from_messages([
        ("system", """You are the VeriSQL Writer Agent.
Repair the previous SQL using the evaluator's feedback.
Use only the supplied SQLite schema. Generate one read-only SELECT query.
Do not repeat the identified mistake.
{format_instructions}"""),
        ("human", """Original question: {question}
Previous SQL: {previous_sql}
Evaluator feedback: {evaluator_feedback}
Relevant columns: {relevant_columns}
Database schema: {schema}
Generate corrected SQL.""")
    ])
    repair_chain = repair_prompt | llm | sql_parser
    return repair_chain.invoke({
        "question": question,
        "previous_sql": previous_sql,
        "evaluator_feedback": evaluator_feedback,
        "relevant_columns": "\n".join(context["relevant_columns"]),
        "schema": context["schema"],
        "format_instructions": format_instructions
    })

def validate_generated_sql(sql_query: str) -> dict:
    try:
        statements = parse(sql_query, read="sqlite")
        if len(statements) != 1 or statements[0] is None:
            return {"valid": False, "reason": "Only one SQL statement is allowed."}
        statement = statements[0]
        if not isinstance(statement, exp.Query):
            return {"valid": False, "reason": "Only read-only SELECT queries are allowed."}
        blocked = (exp.Insert, exp.Update, exp.Delete, exp.Create, exp.Drop, exp.Alter, exp.Command)
        if any(isinstance(node, blocked) for node in statement.walk()):
            return {"valid": False, "reason": "Write or schema-modification operations are not allowed."}
        return {"valid": True, "reason": "SQL passed structural validation."}
    except Exception as error:
        return {"valid": False, "reason": str(error)}

def log_repair(question: str, initial_sql: str, evaluator_feedback: str, repaired_sql: str, initial_validation: dict, final_validation: dict):
    log_entry = {
        "question": question,
        "initial_sql": initial_sql,
        "evaluator_feedback": evaluator_feedback,
        "repaired_sql": repaired_sql,
        "initial_validation": initial_validation,
        "final_validation": final_validation,
        "repair_success": final_validation["valid"]
    }
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(REPAIR_LOG_PATH, "a", encoding="utf-8") as file:
        file.write(json.dumps(log_entry) + "\n")
