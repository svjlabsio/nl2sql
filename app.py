"""
NL2SQL — Natural Language to SQL showcase application.
"""
import re
import os
import sys
import psycopg2

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

# ---------------------------------------------------------------------------
# CSS tweaks
# ---------------------------------------------------------------------------

st.markdown(
    """
    <style>
    /* Widen code blocks */
    .stCodeBlock { font-size: 0.85rem; }
    /* Reduce sidebar padding */
    section[data-testid="stSidebar"] { padding-top: 1rem; }
    </style>
    """,
    unsafe_allow_html=True,
)

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


@st.cache_data(ttl=60)
def load_history(schema_id: str, limit: int = 10) -> list[dict]:
    with db_cursor() as cur:
        cur.execute(
            """SELECT nl_query, generated_sql, thinking, error_message, retry_count, created_at
               FROM query_history WHERE schema_id = %s ORDER BY created_at DESC LIMIT %s""",
            (schema_id, limit),
        )
        return [dict(r) for r in cur.fetchall()]


def invalidate_caches():
    load_schemas.clear()
    load_schema_tables.clear()
    load_table_columns.clear()
    load_history.clear()


# ---------------------------------------------------------------------------
# Import schema logic
# ---------------------------------------------------------------------------


def import_schema_from_ddl(name: str, description: str, dialect: str, ddl: str):
    tables = parse_ddl(ddl)
    if not tables:
        st.error("No CREATE TABLE statements found. Check your DDL syntax.")
        return False

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO registered_schemas (name, description, dialect) VALUES (%s, %s, %s) RETURNING id",
                (name, description, dialect),
            )
            schema_id = cur.fetchone()[0]

            table_embed_pairs = []
            col_embed_pairs = []

            for tdef in tables:
                cur.execute(
                    "INSERT INTO schema_tables (schema_id, name) VALUES (%s, %s) RETURNING id",
                    (schema_id, tdef.name),
                )
                table_id = cur.fetchone()[0]
                table_embed_pairs.append((f"{tdef.name}", table_id))

                for col in tdef.columns:
                    cur.execute(
                        """INSERT INTO schema_columns
                               (table_id, schema_id, name, data_type, is_nullable,
                                is_primary_key, is_foreign_key, fk_references)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id""",
                        (
                            table_id,
                            schema_id,
                            col.name,
                            col.data_type,
                            col.is_nullable,
                            col.is_primary_key,
                            col.is_foreign_key,
                            col.fk_references,
                        ),
                    )
                    col_id = cur.fetchone()[0]
                    col_embed_pairs.append(
                        (f"{tdef.name}.{col.name} ({col.data_type})", col_id)
                    )

            # Generate embeddings
            if table_embed_pairs:
                vecs = embed([t for t, _ in table_embed_pairs])
                for (_, tid), vec in zip(table_embed_pairs, vecs):
                    cur.execute(
                        "UPDATE schema_tables SET embedding = %s::vector WHERE id = %s",
                        (vec, tid),
                    )
            if col_embed_pairs:
                vecs = embed([t for t, _ in col_embed_pairs])
                for (_, cid), vec in zip(col_embed_pairs, vecs):
                    cur.execute(
                        "UPDATE schema_columns SET embedding = %s::vector WHERE id = %s",
                        (vec, cid),
                    )

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
        cur.execute("DELETE FROM registered_schemas WHERE id = %s AND is_demo = FALSE", (schema_id,))
    invalidate_caches()


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------


def render_sidebar(schemas: list[dict]) -> dict | None:
    st.sidebar.markdown("## 🔍 NL2SQL")
    st.sidebar.markdown(
        "Convert plain English to SQL using **Claude** + **pgvector** semantic schema search."
    )
    st.sidebar.markdown("---")

    if not schemas:
        st.sidebar.warning("No schemas found. Run `python scripts/seed_schemas.py` first.")
        return None

    schema_names = [f"{'⭐ ' if s['is_demo'] else ''}{s['name']}" for s in schemas]
    idx = st.sidebar.selectbox("Schema", range(len(schemas)), format_func=lambda i: schema_names[i])
    selected = schemas[idx]

    st.sidebar.caption(selected.get("description") or "")
    st.sidebar.badge(selected["dialect"].upper(), color="blue")

    # Schema browser
    st.sidebar.markdown("**Tables**")
    tables = load_schema_tables(str(selected["id"]))
    for table in tables:
        with st.sidebar.expander(f"📋 {table['name']}", expanded=False):
            cols = load_table_columns(str(table["id"]))
            if cols:
                for col in cols:
                    badges = ""
                    if col["is_primary_key"]:
                        badges += " 🔑"
                    if col["is_foreign_key"]:
                        badges += " 🔗"
                    desc = f" — *{col['description']}*" if col.get("description") else ""
                    st.markdown(
                        f"`{col['name']}` {col['data_type']}{badges}{desc}"
                    )
            else:
                st.caption("No columns found.")

    # Delete non-demo schema
    if not selected["is_demo"]:
        if st.sidebar.button("🗑 Delete this schema", type="secondary"):
            delete_schema(str(selected["id"]))
            st.rerun()

    return selected


# ---------------------------------------------------------------------------
# Query tab
# ---------------------------------------------------------------------------


def render_query_tab(schema: dict):
    st.markdown(f"### Ask anything about **{schema['name']}**")
    st.caption(
        "Uses semantic search to find relevant tables, few-shot examples, and chain-of-thought reasoning."
    )

    # Pre-fill with example queries
    placeholder_map = {
        "E-commerce": "e.g. Show me the top 10 customers by total revenue",
        "HR": "e.g. List all employees in Engineering with their current salary",
    }
    placeholder = placeholder_map.get(schema["name"], "e.g. Show me all records from last month")

    nl_query = st.text_area(
        "Natural language query",
        placeholder=placeholder,
        height=100,
        label_visibility="collapsed",
    )

    col1, col2 = st.columns([1, 6])
    with col1:
        generate = st.button("Generate SQL ⚡", type="primary", use_container_width=True)

    if generate and nl_query.strip():
        _run_and_display(schema, nl_query.strip())
    elif generate:
        st.warning("Please enter a query.")

    # History
    history = load_history(str(schema["id"]))
    if history:
        st.markdown("---")
        st.markdown("#### Recent queries")
        for h in history:
            with st.expander(f"*{h['nl_query'][:80]}{'...' if len(h['nl_query']) > 80 else ''}*"):
                if h.get("thinking"):
                    st.markdown("**Reasoning:**")
                    st.markdown(f"*{h['thinking'][:400]}{'...' if len(h['thinking']) > 400 else ''}*")
                if h.get("generated_sql"):
                    st.code(h["generated_sql"], language="sql")
                if h.get("error_message"):
                    st.error(f"Error: {h['error_message']}")
                if h.get("retry_count", 0) > 0:
                    st.caption(f"⚠ Required {h['retry_count']} retry(s) to fix SQL")
                st.caption(f"{h['created_at'].strftime('%Y-%m-%d %H:%M')}")


def _run_and_display(schema: dict, nl_query: str):
    stream_placeholder = st.empty()
    stream_placeholder.markdown("*⚡ Generating...*")

    # Stream raw tokens to show live progress
    import anthropic as _anthropic
    from lib.schema_pruner import get_schema_context
    from lib.prompt_builder import render_schema_ddl, build_system_prompt
    from lib.sql_validator import validate_and_format

    schema_id = str(schema["id"])
    ctx = get_schema_context(schema_id, nl_query)
    schema_ddl = render_schema_ddl(ctx["tables"], ctx["columns_by_table"])
    system_prompt = build_system_prompt(schema["name"], schema["dialect"], schema_ddl, ctx["examples"])

    client = _anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    buffer = ""

    try:
        with client.messages.stream(
            model="claude-sonnet-4-6",
            max_tokens=2048,
            system=[{"type": "text", "text": system_prompt, "cache_control": {"type": "ephemeral"}}],
            messages=[{"role": "user", "content": nl_query}],
        ) as stream:
            for token in stream.text_stream:
                buffer += token
                # Live display: strip XML tags, show as plain text
                display = (
                    buffer
                    .replace("<thinking>", "💭 *Reasoning:*\n\n")
                    .replace("</thinking>", "\n\n---\n\n")
                    .replace("<sql>", "```sql\n")
                    .replace("</sql>", "\n```")
                )
                stream_placeholder.markdown(display)
    except Exception as e:
        stream_placeholder.error(f"LLM error: {e}")
        return

    # Parse and display properly
    stream_placeholder.empty()

    thinking_m = re.search(r"<thinking>(.*?)</thinking>", buffer, re.DOTALL)
    sql_m = re.search(r"<sql>(.*?)</sql>", buffer, re.DOTALL)
    thinking = thinking_m.group(1).strip() if thinking_m else ""
    raw_sql = sql_m.group(1).strip() if sql_m else buffer.strip()

    is_valid, formatted_sql, error = validate_and_format(raw_sql)

    # Validate + possible 1 retry
    retries = 0
    if not is_valid:
        retries = 1
        retry_msg = st.empty()
        retry_msg.info(f"⚠ SQL parse error — retrying... ({error})")
        result = run_nl2sql(schema_id, schema["name"], schema["dialect"], nl_query)
        retry_msg.empty()
        formatted_sql = result["sql"]
        thinking = result.get("thinking") or thinking
        error = result.get("error")
        retries = result.get("retries", 1)

    # Display SQL
    if formatted_sql:
        st.markdown("#### Generated SQL")
        st.code(formatted_sql, language="sql")

        c1, c2 = st.columns([1, 5])
        with c1:
            st.button("📋 Copy", help="Copy the SQL above", disabled=True)  # visual affordance

        if error:
            st.warning(f"⚠ Could not fully validate SQL: {error}")
        if retries > 0:
            st.caption(f"Used {retries} auto-correction retry(s)")

    # Display reasoning
    if thinking:
        with st.expander("💭 View reasoning", expanded=False):
            st.markdown(thinking)

    # Log to history
    from lib.nl2sql_pipeline import _save_history
    _save_history(schema_id, nl_query, formatted_sql or raw_sql, thinking, error, retries)
    load_history.clear()


# ---------------------------------------------------------------------------
# Import tab
# ---------------------------------------------------------------------------


def render_import_tab():
    st.markdown("### Import a schema")
    st.markdown(
        "Paste your `CREATE TABLE` DDL statements below. "
        "Tables and columns will be embedded for semantic search."
    )

    col1, col2 = st.columns(2)
    with col1:
        schema_name = st.text_input("Schema name *", placeholder="My Database")
    with col2:
        dialect = st.selectbox("SQL dialect", ["postgresql", "mysql", "bigquery", "snowflake"])

    description = st.text_input("Description (optional)", placeholder="What this database is about")

    ddl = st.text_area(
        "Paste DDL",
        height=300,
        placeholder="""CREATE TABLE users (
  id UUID PRIMARY KEY,
  email TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE posts (
  id UUID PRIMARY KEY,
  user_id UUID REFERENCES users(id),
  title TEXT NOT NULL,
  body TEXT,
  published_at TIMESTAMPTZ
);""",
    )

    # Live preview of parsed tables
    if ddl.strip():
        parsed = parse_ddl(ddl)
        if parsed:
            st.markdown(f"**Preview:** {len(parsed)} table(s) detected")
            for tdef in parsed:
                st.caption(f"📋 {tdef.name} ({len(tdef.columns)} columns)")
        else:
            st.caption("No CREATE TABLE statements detected yet.")

    if st.button("Import Schema ⬆", type="primary", disabled=not (schema_name and ddl.strip())):
        if not schema_name:
            st.error("Schema name is required.")
        elif not ddl.strip():
            st.error("DDL cannot be empty.")
        else:
            with st.spinner("Parsing DDL and generating embeddings..."):
                success = import_schema_from_ddl(schema_name, description, dialect, ddl)
            if success:
                st.success(f"✅ Schema **{schema_name}** imported. Select it from the sidebar.")
                st.rerun()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    schemas = load_schemas()
    selected_schema = render_sidebar(schemas)

    tab_query, tab_import = st.tabs(["🔍 Query Playground", "⬆ Import Schema"])

    with tab_query:
        if selected_schema:
            render_query_tab(selected_schema)
        else:
            st.info("No schemas available. Import one using the **Import Schema** tab.")

    with tab_import:
        render_import_tab()


main()
