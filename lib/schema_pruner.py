from pgvector.psycopg2 import register_vector
from lib.db import db_cursor, get_conn, put_conn
from lib.embeddings import embed_one


def get_relevant_tables(schema_id: str, query: str, top_k: int = 15) -> list[dict]:
    """Return the top-k tables most relevant to the query using pgvector cosine similarity."""
    query_vec = embed_one(query)
    conn = get_conn()
    try:
        register_vector(conn)
        with conn.cursor() as cur:
            from psycopg2.extras import RealDictCursor
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute(
                """
                SELECT id, name, description
                FROM schema_tables
                WHERE schema_id = %s AND embedding IS NOT NULL
                ORDER BY embedding <=> %s::vector
                LIMIT %s
                """,
                (schema_id, query_vec, top_k),
            )
            tables = cur.fetchall()
            if not tables:
                # Fallback when no embeddings exist yet
                cur.execute(
                    "SELECT id, name, description FROM schema_tables WHERE schema_id = %s LIMIT %s",
                    (schema_id, top_k),
                )
                tables = cur.fetchall()
            cur.close()
        conn.commit()
        return [dict(t) for t in tables]
    finally:
        put_conn(conn)


def get_columns_for_tables(table_ids: list[str]) -> dict[str, list[dict]]:
    """Return columns grouped by table_id."""
    if not table_ids:
        return {}
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT table_id, name, data_type, is_nullable,
                   is_primary_key, is_foreign_key, fk_references, description
            FROM schema_columns
            WHERE table_id = ANY(%s::uuid[])
            ORDER BY table_id, is_primary_key DESC, name
            """,
            (table_ids,),
        )
        rows = cur.fetchall()

    grouped: dict[str, list[dict]] = {}
    for row in rows:
        tid = str(row["table_id"])
        grouped.setdefault(tid, []).append(dict(row))
    return grouped


def get_few_shot_examples(schema_id: str, query: str, top_k: int = 3) -> list[dict]:
    """Return top-k semantically similar few-shot examples."""
    query_vec = embed_one(query)
    conn = get_conn()
    try:
        register_vector(conn)
        with conn.cursor() as cur:
            from psycopg2.extras import RealDictCursor
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute(
                """
                SELECT nl_query, sql_query
                FROM few_shot_examples
                WHERE schema_id = %s AND embedding IS NOT NULL
                ORDER BY embedding <=> %s::vector
                LIMIT %s
                """,
                (schema_id, query_vec, top_k),
            )
            rows = cur.fetchall()
            cur.close()
        conn.commit()
        return [dict(r) for r in rows]
    finally:
        put_conn(conn)


def get_schema_context(schema_id: str, query: str) -> dict:
    """Full context bundle: relevant tables + their columns + few-shot examples."""
    tables = get_relevant_tables(schema_id, query)
    table_ids = [str(t["id"]) for t in tables]
    columns_by_table = get_columns_for_tables(table_ids)
    examples = get_few_shot_examples(schema_id, query)
    return {
        "tables": tables,
        "columns_by_table": columns_by_table,
        "examples": examples,
    }
