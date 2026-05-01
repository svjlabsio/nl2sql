"""
NL2SQL — Natural Language to SQL showcase application.
"""
import re
import os
import sys

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(__file__))
load_dotenv()

from lib.db import db_cursor, get_conn, put_conn
from lib.nl2sql_pipeline import run_nl2sql
from lib.ddl_parser import parse_ddl
from lib.embeddings import embed

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="NL2SQL",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
.stCodeBlock { font-size: 0.85rem; }
section[data-testid="stSidebar"] { padding-top: 1rem; }
.prompt-btn button { text-align: left !important; }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Sample prompts per schema
# ---------------------------------------------------------------------------

SAMPLE_PROMPTS = {
    "E-commerce": [
        ("Simple",  "How many customers do we have in total?"),
        ("Simple",  "Which products are currently out of stock?"),
        ("Complex", "Show the top 5 customers by lifetime revenue — include order count, average order value, and days since their last order"),
        ("Complex", "What is the month-over-month revenue percentage change for the last 6 months?"),
    ],
    "HR": [
        ("Simple",  "How many active employees are in each department?"),
        ("Simple",  "List all job titles with their minimum and maximum salary bands"),
        ("Complex", "Find employees whose current salary exceeds their job title's maximum salary band — show how much they are over"),
        ("Complex", "For each department show headcount, average salary, and the percentage of employees who scored 4 or higher in their most recent performance review"),
    ],
}

# ---------------------------------------------------------------------------
# DB helpers (cached)
# ---------------------------------------------------------------------------


@st.cache_data(ttl=60)
def load_schemas() -> list[dict]:
    with db_cursor() as cur:
        cur.execute(
            "SELECT id, name, description, dialect, is_demo FROM registered_schemas ORDER BY is_demo DESC, name"
        )
        return [dict(r) for r in cur.fetchall()]


@st.cache_data(ttl=60)
def load_schema_tables(schema_id: str) -> list[dict]:
    with db_cursor() as cur:
        cur.execute(
            "SELECT id, name, description FROM schema_tables WHERE schema_id = %s ORDER BY name",
            (schema_id,),
        )
        return [dict(r) for r in cur.fetchall()]


@st.cache_data(ttl=60)
def load_table_columns(table_id: str) -> list[dict]:
    with db_cursor() as cur:
        cur.execute(
            """SELECT name, data_type, is_primary_key, is_foreign_key, fk_references, description
               FROM schema_columns WHERE table_id = %s ORDER BY is_primary_key DESC, name""",
            (table_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def invalidate_caches():
    load_schemas.clear()
    load_schema_tables.clear()
    load_table_columns.clear()


# ---------------------------------------------------------------------------
# SQL execution
# ---------------------------------------------------------------------------

import sqlparse as _sqlparse


def execute_sql(sql: str) -> tuple[pd.DataFrame | None, str | None]:
    """Execute a SELECT query against Neon and return (dataframe, error)."""
    try:
        stmt_type = _sqlparse.parse(sql)[0].get_type()
    except Exception:
        stmt_type = None

    if stmt_type != "SELECT":
        return None, "Only SELECT queries are executed automatically."

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            col_names = [d[0] for d in cur.description]
            rows = cur.fetchmany(300)
        conn.commit()
        df = pd.DataFrame(rows, columns=col_names)
        return df, None
    except Exception as e:
        conn.rollback()
        return None, str(e)
    finally:
        put_conn(conn)


# ---------------------------------------------------------------------------
# Import schema logic
# ---------------------------------------------------------------------------


def import_schema_from_ddl(name: str, description: str, dialect: str, ddl: str) -> bool:
    tables = parse_ddl(ddl)
    if not tables:
        st.error("No CREATE TABLE statements found.")
        return False

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO registered_schemas (name, description, dialect) VALUES (%s, %s, %s) RETURNING id",
                (name, description, dialect),
            )
            schema_id = cur.fetchone()[0]

            table_pairs, col_pairs = [], []
            for tdef in tables:
                cur.execute(
                    "INSERT INTO schema_tables (schema_id, name) VALUES (%s, %s) RETURNING id",
                    (schema_id, tdef.name),
                )
                tid = cur.fetchone()[0]
                table_pairs.append((tdef.name, tid))
                for col in tdef.columns:
                    cur.execute(
                        """INSERT INTO schema_columns
                               (table_id, schema_id, name, data_type, is_nullable,
                                is_primary_key, is_foreign_key, fk_references)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
                        (tid, schema_id, col.name, col.data_type, col.is_nullable,
                         col.is_primary_key, col.is_foreign_key, col.fk_references),
                    )
                    cid = cur.fetchone()[0]
                    col_pairs.append((f"{tdef.name}.{col.name} ({col.data_type})", cid))

            if table_pairs:
                vecs = embed([t for t, _ in table_pairs])
                for (_, tid), vec in zip(table_pairs, vecs):
                    cur.execute("UPDATE schema_tables SET embedding=%s::vector WHERE id=%s", (vec, tid))
            if col_pairs:
                vecs = embed([t for t, _ in col_pairs])
                for (_, cid), vec in zip(col_pairs, vecs):
                    cur.execute("UPDATE schema_columns SET embedding=%s::vector WHERE id=%s", (vec, cid))

        conn.commit()
        invalidate_caches()
        return True
    except Exception as e:
        conn.rollback()
        st.error(f"Import failed: {e}")
        return False
    finally:
        put_conn(conn)


def delete_schema(schema_id: str):
    with db_cursor() as cur:
        cur.execute("DELETE FROM registered_schemas WHERE id=%s AND is_demo=FALSE", (schema_id,))
    invalidate_caches()


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------


def render_sidebar(schemas: list[dict]) -> dict | None:
    st.sidebar.markdown("## 🔍 NL2SQL")
    st.sidebar.markdown(
        "Plain English → SQL using **Claude** + **pgvector** semantic schema search."
    )
    st.sidebar.markdown("---")

    if not schemas:
        st.sidebar.warning("No schemas found. Run `python scripts/seed_schemas.py`.")
        return None

    schema_names = [f"{'⭐ ' if s['is_demo'] else ''}{s['name']}" for s in schemas]
    idx = st.sidebar.selectbox("Schema", range(len(schemas)), format_func=lambda i: schema_names[i])
    selected = schemas[idx]
    st.sidebar.caption(selected.get("description") or "")
    st.sidebar.badge(selected["dialect"].upper(), color="blue")

    st.sidebar.markdown("**Tables**")
    for table in load_schema_tables(str(selected["id"])):
        with st.sidebar.expander(f"📋 {table['name']}", expanded=False):
            for col in load_table_columns(str(table["id"])):
                badges = (" 🔑" if col["is_primary_key"] else "") + (" 🔗" if col["is_foreign_key"] else "")
                desc = f" — *{col['description']}*" if col.get("description") else ""
                st.markdown(f"`{col['name']}` {col['data_type']}{badges}{desc}")

    if not selected["is_demo"]:
        if st.sidebar.button("🗑 Delete schema", type="secondary"):
            delete_schema(str(selected["id"]))
            st.rerun()

    return selected


# ---------------------------------------------------------------------------
# Core NL2SQL generation + display (session-state backed)
# ---------------------------------------------------------------------------


def _run_query(schema: dict, nl_query: str):
    """
    Stream the NL2SQL pipeline into a two-column layout (SQL left, reasoning right),
    then persist the finished result in st.session_state.
    """
    import anthropic as _anthropic
    from lib.schema_pruner import get_schema_context
    from lib.prompt_builder import render_schema_ddl, build_system_prompt
    from lib.sql_validator import validate_and_format
    from lib.nl2sql_pipeline import _save_history

    schema_id = str(schema["id"])
    ctx = get_schema_context(schema_id, nl_query)
    schema_ddl = render_schema_ddl(ctx["tables"], ctx["columns_by_table"])
    system_prompt = build_system_prompt(schema["name"], schema["dialect"], schema_ddl, ctx["examples"])

    client = _anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    st.markdown("**Generated SQL**")
    sql_ph = st.empty()
    sql_ph.markdown("*⚡ Generating...*")

    raw_buf = ""

    try:
        with client.messages.stream(
            model="claude-sonnet-4-6",
            max_tokens=2048,
            system=[{"type": "text", "text": system_prompt, "cache_control": {"type": "ephemeral"}}],
            messages=[{"role": "user", "content": nl_query}],
        ) as stream:
            for token in stream.text_stream:
                raw_buf += token
                #s0 = raw_buf.find("<sql>")
                #s1 = raw_buf.find("</sql>")
                #if s0 >= 0:
                #    sql_text = raw_buf[s0 + len("<sql>"): s1 if s1 >= 0 else len(raw_buf)]
                #    if sql_text.strip():
                #        sql_ph.code(sql_text.strip(), language="sql")

    except Exception as e:
        sql_ph.error(f"LLM error: {e}")
        return

    # Parse final buffer
    thinking_m = re.search(r"<thinking>(.*?)</thinking>", raw_buf, re.DOTALL)
    sql_m = re.search(r"<sql>(.*?)</sql>", raw_buf, re.DOTALL)
    thinking = thinking_m.group(1).strip() if thinking_m else ""
    raw_sql = sql_m.group(1).strip() if sql_m else raw_buf.strip()

    is_valid, fmt_sql, error = validate_and_format(raw_sql)

    retries = 0
    if not is_valid:
        with st.spinner(f"Auto-correcting SQL... ({error})"):
            fixed = run_nl2sql(schema_id, schema["name"], schema["dialect"], nl_query)
        fmt_sql = fixed["sql"]
        thinking = fixed.get("thinking") or thinking
        error = fixed.get("error")
        retries = fixed.get("retries", 1)

    _save_history(schema_id, nl_query, fmt_sql or raw_sql, thinking, error, retries)

    st.session_state["result"] = {
        "query": nl_query,
        "sql": fmt_sql or raw_sql,
        "thinking": thinking,
        "error": error,
        "retries": retries,
    }


def _display_result(result: dict):
    """Render a persisted result: SQL then query results."""
    st.caption(f"Query: *{result['query']}*")

    st.markdown("**Generated SQL**")
    st.code(result["sql"], language="sql")

    if result.get("retries", 0) > 0:
        st.caption(f"⚠ {result['retries']} auto-correction retry(s) needed")
    if result.get("error"):
        st.warning(f"Validation: {result['error']}")

    st.markdown("**Results**")
    with st.spinner("Running query..."):
        df, exec_error = execute_sql(result["sql"])

    if exec_error:
        st.error(f"Execution error: {exec_error}")
    elif df is None or df.empty:
        st.info("Query returned no rows.")
    else:
        st.caption(f"{len(df)} row(s) — max 300 shown")
        st.dataframe(df, use_container_width=True, hide_index=True)


# ---------------------------------------------------------------------------
# Query tab
# ---------------------------------------------------------------------------


def render_query_tab(schema: dict):
    schema_id = str(schema["id"])
    schema_name = schema["name"]

    # Sample prompts
    prompts = SAMPLE_PROMPTS.get(schema_name, [])
    # Custom query input
    nl_query = st.text_area(
        "Ask a question about your data:",
        height=80,
        placeholder="e.g. Show me the revenue breakdown by category for this year",
        label_visibility="visible",
    )
    if st.button("Generate SQL ⚡", type="primary"):
        if nl_query.strip():
            st.session_state.pop("result", None)
            _run_query(schema, nl_query.strip())
        else:
            st.warning("Please enter a query.")

    # Sample prompts below the input
    if prompts:
        st.markdown("**Try a sample query:**")
        cols = st.columns(2)
        for i, (complexity, prompt_text) in enumerate(prompts):
            color = "🟢" if complexity == "Simple" else "🔴"
            with cols[i % 2]:
                if st.button(
                    f"{color} **{complexity}** — {prompt_text}",
                    key=f"prompt_{schema_id}_{i}",
                    use_container_width=True,
                ):
                    st.session_state.pop("result", None)
                    _run_query(schema, prompt_text)

        st.markdown("---")

    # Display persisted result
    if "result" in st.session_state:
        st.markdown("---")
        _display_result(st.session_state["result"])


# ---------------------------------------------------------------------------
# Import tab
# ---------------------------------------------------------------------------


def render_import_tab():
    st.markdown("### Import a schema")
    st.markdown("Paste your `CREATE TABLE` DDL. Tables and columns will be embedded for semantic search.")

    col1, col2 = st.columns(2)
    with col1:
        schema_name = st.text_input("Schema name *", placeholder="My Database")
    with col2:
        dialect = st.selectbox("SQL dialect", ["postgresql", "mysql", "bigquery", "snowflake"])
    description = st.text_input("Description (optional)")

    ddl = st.text_area("Paste DDL", height=280, placeholder="CREATE TABLE users (\n  id UUID PRIMARY KEY,\n  ...\n);")

    if ddl.strip():
        parsed = parse_ddl(ddl)
        st.caption(f"Detected: {len(parsed)} table(s)" if parsed else "No CREATE TABLE statements detected yet.")

    if st.button("Import ⬆", type="primary", disabled=not (schema_name and ddl.strip())):
        with st.spinner("Parsing and generating embeddings..."):
            ok = import_schema_from_ddl(schema_name, description, dialect, ddl)
        if ok:
            st.success(f"✅ Schema **{schema_name}** imported. Select it from the sidebar.")
            st.rerun()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    schemas = load_schemas()
    selected = render_sidebar(schemas)

    tab_query, tab_import = st.tabs(["🔍 Query Playground", "⬆ Import Schema"])

    with tab_query:
        if selected:
            render_query_tab(selected)
        else:
            st.info("No schemas found. Import one or run the seed scripts.")

    with tab_import:
        render_import_tab()


main()
