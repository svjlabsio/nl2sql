# NL2SQL — Natural Language to SQL

A portfolio showcase that converts plain-English questions into SQL queries using Claude, semantic schema search, and prompt caching. Load a pre-built demo schema or paste your own DDL and immediately start querying.

## Live demo

Two schemas are pre-loaded: **E-commerce** (customers, orders, products) and **HR** (employees, departments, salaries). Try questions ranging from `"How many customers do we have?"` to complex multi-join aggregations like `"What is the month-over-month revenue percentage change for the last 6 months?"`.

Enable **cross-schema mode** in the sidebar to query across both schemas simultaneously — Claude sees the merged DDL and writes SQL that joins tables from different schemas in a single query (e.g. comparing revenue growth with headcount changes).

---

## Tech stack

| Layer | Choice |
|---|---|
| UI | Streamlit |
| LLM | Claude claude-sonnet-4-6 (Anthropic SDK, streaming) |
| Embeddings | `all-MiniLM-L6-v2` via sentence-transformers (local, 384-dim, no API key) |
| Database | Neon Postgres + pgvector |
| SQL validation | sqlparse |
| DDL parsing | Custom regex parser |

---

## Core AI pipeline

```
User question
      │
      ▼
┌─────────────────────────────────────────────────────────────────┐
│  Step 1 · Embed the question                                    │
│                                                                 │
│  all-MiniLM-L6-v2 encodes the NL query into a 384-dim vector   │
│  locally, with no external API call.                            │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  Step 2 · Schema pruning (pgvector cosine similarity)           │
│                                                                 │
│  "SELECT id, name FROM schema_tables                            │
│   WHERE schema_id = ?                                           │
│   ORDER BY embedding <=> query_vec LIMIT 15"                    │
│                                                                 │
│  Every table and column was pre-embedded at import time.        │
│  Only the 15 most semantically relevant tables are passed to    │
│  the LLM — preventing context overflow on large schemas.        │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  Step 3 · Few-shot example retrieval (pgvector)                 │
│                                                                 │
│  "SELECT nl_query, sql_query FROM few_shot_examples             │
│   WHERE schema_id = ?                                           │
│   ORDER BY embedding <=> query_vec LIMIT 3"                     │
│                                                                 │
│  Retrieves the 3 curated NL→SQL examples most similar to the   │
│  current question and injects them into the prompt.             │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  Step 4 · Prompt assembly with Anthropic prompt caching         │
│                                                                 │
│  System prompt (marked cache_control: ephemeral):              │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │ Role + dialect-specific SQL rules                         │ │
│  │ Pruned schema DDL (only relevant tables)                  │ │
│  │ 3 few-shot NL→SQL examples                                │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                 │
│  Prompt caching means the schema tokens are billed only on the  │
│  first call per session (~80% token cost reduction thereafter). │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  Step 5 · Claude claude-sonnet-4-6 call (streaming)             │
│                                                                 │
│  Model emits two tagged sections:                               │
│  <thinking> step-by-step reasoning </thinking>                  │
│  <sql> final SQL query </sql>                                   │
│                                                                 │
│  SQL tokens stream live into the left column as they arrive.    │
│  Reasoning appears in a fixed-height scrollable panel on the    │
│  right once the stream completes.                               │
│  max_tokens=4096 to accommodate long thinking blocks on         │
│  complex queries without truncating the <sql> output.           │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  Step 6 · SQL validation + error recovery                       │
│                                                                 │
│  sqlparse.parse() validates the generated SQL.                  │
│  On failure → feed the error back to Claude as a follow-up      │
│  message and retry (up to 2 retries):                           │
│                                                                 │
│  "That SQL has a parse error: <error>                           │
│   Please fix it and output only the corrected SQL."             │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  Step 7 · Execute + display                                     │
│                                                                 │
│  Final SQL runs against Neon Postgres.                          │
│  Write operations are blocked by first-keyword check            │
│  (SELECT / WITH allowed; INSERT, UPDATE, DELETE, DROP, etc.     │
│  rejected). CTEs (WITH ... SELECT) execute correctly.           │
│  Up to 300 rows returned and rendered as a Streamlit dataframe. │
│  Query logged to query_history table.                           │
└─────────────────────────────────────────────────────────────────┘
```

### Cross-schema mode pipeline

When the sidebar toggle is on, Step 2–4 change:

```
Step 2 · Schema pruning runs for EACH selected schema independently
         Top-15 relevant tables retrieved per schema via pgvector

Step 3 · Few-shot retrieval runs per schema; results are merged
         (capped at 3 total examples passed to the LLM)

Step 4 · DDLs from all schemas are concatenated into one system prompt
         Schema name shown as "E-commerce + HR" (or whichever are selected)
         Single Claude call generates SQL that can JOIN across all tables
         (valid because all tables live in the same Neon database)

Steps 5–7 unchanged — one SQL, one result set
```

---

## How schema import works

```
Paste DDL
    │
    ▼
DDL parser (regex)
    │  Extracts: table names, column names, data types,
    │  PRIMARY KEY, FOREIGN KEY, NOT NULL constraints
    ▼
Insert into registered_schemas → schema_tables → schema_columns (Neon)
    │
    ▼
Batch embed all table names + column signatures
    │  e.g. "orders.customer_id (UUID)" → 384-dim vector
    ▼
Store vectors in pgvector (HNSW index)
    │
    ▼
Schema is immediately queryable
```

Supported dialects: PostgreSQL · MySQL · BigQuery · Snowflake

---

## Project structure

```
nl2sql/
├── app.py                        # Streamlit UI — streaming, results, import tab
├── lib/
│   ├── db.py                     # psycopg2 ThreadedConnectionPool (1–10 conns)
│   ├── embeddings.py             # sentence-transformers wrapper (local, no API key)
│   ├── ddl_parser.py             # Regex DDL parser → TableDef / ColumnDef
│   ├── schema_pruner.py          # pgvector cosine search for tables + few-shots
│   ├── prompt_builder.py         # Prompt assembly + dialect-specific SQL rules
│   ├── sql_validator.py          # sqlparse validation + formatting
│   └── nl2sql_pipeline.py        # Main orchestrator — retry loop + history logging
└── scripts/
    ├── init_db.py                # Creates Postgres tables + HNSW indexes
    ├── seed_schemas.py           # Seeds E-commerce + HR schemas with embeddings
    └── seed_data.py              # Inserts realistic sample data for both schemas
```

---

## Database schema

```sql
registered_schemas   -- schema registry (name, dialect, is_demo flag)
    │
    ├── schema_tables            -- one row per table; embedding VECTOR(384)
    │       │
    │       └── schema_columns   -- one row per column; embedding VECTOR(384)
    │
    ├── few_shot_examples        -- curated NL→SQL pairs; embedding VECTOR(384)
    │
    └── query_history            -- audit log (nl_query, generated_sql, retries)

-- HNSW indexes on all three embedding columns for fast cosine search
CREATE INDEX USING hnsw (embedding vector_cosine_ops);
```

---

## Setup

### Prerequisites

- Python 3.11+
- A [Neon](https://neon.tech) Postgres database (free tier)
- An [Anthropic](https://console.anthropic.com) API key

### 1. Clone and install

```bash
git clone <repo-url>
cd nl2sql
pip install -r requirements.txt
```

### 2. Configure environment

Create a `.env` file:

```
DATABASE_URL=postgresql://...  # Neon connection string
ANTHROPIC_API_KEY=sk-ant-...
```

### 3. Initialize the database

```bash
python scripts/init_db.py
```

### 4. Seed demo schemas and data

```bash
python scripts/seed_schemas.py   # Creates E-commerce + HR schemas + embeddings
python scripts/seed_data.py      # Inserts sample data (40 customers, 27 employees, etc.)
```

`all-MiniLM-L6-v2` (~90 MB) downloads automatically on first run and is cached locally.

### 5. Run the app

```bash
streamlit run app.py
```

---

## Key design decisions

**Local embeddings, no OpenAI key** — `all-MiniLM-L6-v2` runs on CPU with sentence-transformers. 384-dim vectors are compact enough for HNSW indexes and fast enough for real-time query embedding. Zero cost, zero external dependency for embeddings.

**Schema pruning prevents context overflow** — On large schemas with dozens of tables, sending everything to the LLM wastes tokens and degrades accuracy. pgvector cosine search selects only the top-15 most relevant tables per query, keeping the prompt tight.

**Prompt caching cuts costs ~80%** — The schema DDL is the largest part of the prompt and rarely changes within a session. Marking the system prompt with `cache_control: ephemeral` means Anthropic caches it server-side; subsequent queries in the same session pay only for the user message tokens.

**Retry loop with error feedback** — If sqlparse rejects the generated SQL, the parse error is fed back to Claude as a follow-up message ("That SQL has a parse error: ..."). This multi-turn correction typically resolves syntax issues in one retry without requiring a full re-prompt.

**Cross-schema queries via DDL merging** — Enabling cross-schema mode doesn't run separate queries per schema. Instead, the top-15 relevant tables from every selected schema are merged into a single system prompt, and Claude writes one SQL query that can JOIN across all of them. This works because all logical schemas share the same physical Neon database.

**CTE-safe execution guard** — The write-operation block uses a first-keyword check (`WITH` and `SELECT` pass; `INSERT`, `UPDATE`, `DELETE`, `DROP`, etc. are rejected) rather than `sqlparse.get_type()`, which misclassifies CTEs as type `None`.
