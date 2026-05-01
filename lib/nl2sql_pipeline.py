import os
import re
import anthropic
from dotenv import load_dotenv

from lib.schema_pruner import get_schema_context
from lib.prompt_builder import render_schema_ddl, build_system_prompt
from lib.sql_validator import validate_and_format
from lib.db import db_cursor

load_dotenv()

_client: anthropic.Anthropic | None = None


def _get_client() -> anthropic.Anthropic:
    global _client
    if _client is None:
        _client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    return _client


def _extract_parts(text: str) -> tuple[str, str]:
    """Extract thinking and sql from the model response."""
    thinking_m = re.search(r"<thinking>(.*?)</thinking>", text, re.DOTALL)
    sql_m = re.search(r"<sql>(.*?)</sql>", text, re.DOTALL)
    thinking = thinking_m.group(1).strip() if thinking_m else ""
    sql = sql_m.group(1).strip() if sql_m else text.strip()
    return thinking, sql


def _call_llm(system_prompt: str, messages: list[dict]) -> str:
    """Call Claude with prompt caching on the system prompt."""
    full_text = ""
    with _get_client().messages.stream(
        model="claude-sonnet-4-6",
        max_tokens=2048,
        system=[
            {
                "type": "text",
                "text": system_prompt,
                "cache_control": {"type": "ephemeral"},
            }
        ],
        messages=messages,
    ) as stream:
        for token in stream.text_stream:
            full_text += token
    return full_text


def run_nl2sql(
    schema_id: str,
    schema_name: str,
    dialect: str,
    nl_query: str,
) -> dict:
    """
    Full NL2SQL pipeline with up to 2 error-recovery retries.

    Returns:
        sql: formatted SQL string
        thinking: chain-of-thought reasoning
        error: None on success, error message on failure
        retries: number of retries used
    """
    ctx = get_schema_context(schema_id, nl_query)
    schema_ddl = render_schema_ddl(ctx["tables"], ctx["columns_by_table"])
    system_prompt = build_system_prompt(schema_name, dialect, schema_ddl, ctx["examples"])

    messages = [{"role": "user", "content": nl_query}]
    last_sql = ""
    last_error = ""

    for attempt in range(3):
        if attempt > 0:
            messages = [
                {"role": "user", "content": nl_query},
                {"role": "assistant", "content": f"<sql>{last_sql}</sql>"},
                {
                    "role": "user",
                    "content": f"That SQL has a parse error: {last_error}\nPlease fix it and output only the corrected SQL inside <sql> tags.",
                },
            ]

        raw = _call_llm(system_prompt, messages)
        thinking, sql = _extract_parts(raw)
        is_valid, formatted_sql, error = validate_and_format(sql)

        if is_valid:
            _save_history(schema_id, nl_query, formatted_sql, thinking, None, attempt)
            return {"sql": formatted_sql, "thinking": thinking, "error": None, "retries": attempt}

        last_sql = sql
        last_error = error or "Unknown parse error"

    _save_history(schema_id, nl_query, last_sql, thinking, last_error, 2)
    return {"sql": last_sql, "thinking": thinking, "error": last_error, "retries": 2}


def _save_history(schema_id, nl_query, sql, thinking, error, retries):
    try:
        with db_cursor() as cur:
            cur.execute(
                """
                INSERT INTO query_history
                    (schema_id, nl_query, generated_sql, thinking, error_message, retry_count)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (schema_id, nl_query, sql, thinking, error, retries),
            )
    except Exception:
        pass  # History is best-effort; don't crash the app on a logging failure
