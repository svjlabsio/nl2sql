"""
Run this once to create all tables and indexes in Neon.
Usage: python scripts/init_db.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import psycopg2
from dotenv import load_dotenv

load_dotenv()

DDL = """
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS registered_schemas (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        TEXT NOT NULL,
    description TEXT,
    dialect     TEXT NOT NULL DEFAULT 'postgresql',
    is_demo     BOOLEAN DEFAULT FALSE,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS schema_tables (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    schema_id   UUID NOT NULL REFERENCES registered_schemas(id) ON DELETE CASCADE,
    name        TEXT NOT NULL,
    description TEXT,
    embedding   VECTOR(1536),
    UNIQUE (schema_id, name)
);

CREATE TABLE IF NOT EXISTS schema_columns (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    table_id       UUID NOT NULL REFERENCES schema_tables(id) ON DELETE CASCADE,
    schema_id      UUID NOT NULL,
    name           TEXT NOT NULL,
    data_type      TEXT NOT NULL,
    is_nullable    BOOLEAN DEFAULT TRUE,
    is_primary_key BOOLEAN DEFAULT FALSE,
    is_foreign_key BOOLEAN DEFAULT FALSE,
    fk_references  TEXT,
    description    TEXT,
    embedding      VECTOR(1536),
    UNIQUE (table_id, name)
);

CREATE TABLE IF NOT EXISTS few_shot_examples (
    id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    schema_id UUID NOT NULL REFERENCES registered_schemas(id) ON DELETE CASCADE,
    nl_query  TEXT NOT NULL,
    sql_query TEXT NOT NULL,
    embedding VECTOR(1536)
);

CREATE TABLE IF NOT EXISTS query_history (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    schema_id     UUID NOT NULL REFERENCES registered_schemas(id),
    nl_query      TEXT NOT NULL,
    generated_sql TEXT,
    thinking      TEXT,
    error_message TEXT,
    retry_count   INT DEFAULT 0,
    created_at    TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tables_embedding
    ON schema_tables USING hnsw (embedding vector_cosine_ops);

CREATE INDEX IF NOT EXISTS idx_columns_embedding
    ON schema_columns USING hnsw (embedding vector_cosine_ops);

CREATE INDEX IF NOT EXISTS idx_examples_embedding
    ON few_shot_examples USING hnsw (embedding vector_cosine_ops);
"""


def main():
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    try:
        with conn.cursor() as cur:
            cur.execute(DDL)
        conn.commit()
        print("Database initialized successfully.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
