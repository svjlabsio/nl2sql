# How NL2SQL Works — A Technical Deep Dive

This document explains the internals of the NL2SQL application for software engineers who are familiar with web backends and databases but have not worked with AI or machine learning systems before. No prior AI knowledge is assumed.

---

## Table of Contents

1. [What problem are we solving?](#1-what-problem-are-we-solving)
2. [High-level architecture](#2-high-level-architecture)
3. [What is an embedding?](#3-what-is-an-embedding)
4. [Vector search with pgvector](#4-vector-search-with-pgvector)
5. [Schema pruning — why we can't send everything to the LLM](#5-schema-pruning)
6. [Few-shot examples — teaching by demonstration](#6-few-shot-examples)
7. [What is an LLM and how does Claude work?](#7-what-is-an-llm-and-how-does-claude-work)
8. [Prompt engineering — how we talk to Claude](#8-prompt-engineering)
9. [Prompt caching — reducing cost](#9-prompt-caching)
10. [Streaming — how tokens appear in real time](#10-streaming)
11. [SQL validation and error recovery](#11-sql-validation-and-error-recovery)
12. [Cross-schema mode](#12-cross-schema-mode)
13. [The complete request lifecycle](#13-the-complete-request-lifecycle)
14. [Data flow diagram](#14-data-flow-diagram)

---

## 1. What problem are we solving?

Most business data lives in relational databases. Getting answers from that data requires writing SQL, which non-technical users cannot do. At the same time, modern AI language models are very good at understanding natural English and generating code.

The goal is to bridge these two things: take a plain-English question like *"Show me the top 5 customers by revenue this year"* and reliably convert it into valid SQL that can be run against the actual database.

The key challenges are:

- **The model does not know your schema.** A general-purpose AI has no idea what tables and columns your specific database has. We need to tell it.
- **Schemas can be very large.** A real-world database might have hundreds of tables. Sending all of them to the AI on every query is expensive and degrades accuracy.
- **The model can hallucinate column names.** Without grounding, the model might reference tables or columns that do not exist.
- **Generated SQL can have syntax errors.** We need a way to detect and correct these automatically.

---

## 2. High-level architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Streamlit UI (app.py)                 │
│  - Text input for natural language query                 │
│  - Schema browser in sidebar                             │
│  - Streams SQL live as Claude generates it               │
│  - Runs SQL and shows results in a dataframe             │
└────────────────────┬────────────────────────────────────┘
                     │
          ┌──────────▼──────────┐
          │   NL2SQL Pipeline   │
          │  (lib/nl2sql_       │
          │   pipeline.py)      │
          └──────────┬──────────┘
          ┌──────────┼──────────────────────┐
          │          │                      │
   ┌──────▼──────┐  ┌▼────────────┐  ┌─────▼──────────┐
   │  Embeddings │  │   pgvector  │  │  Anthropic API  │
   │  (local     │  │  (Neon      │  │  Claude         │
   │  sentence-  │  │  Postgres)  │  │  claude-sonnet- │
   │  transform.)│  │             │  │  4-6            │
   └─────────────┘  └─────────────┘  └────────────────┘
```

**Key components:**

| Component | What it does |
|---|---|
| `lib/embeddings.py` | Converts text into vectors (numbers) for semantic search |
| `lib/schema_pruner.py` | Uses pgvector to find the most relevant tables for a query |
| `lib/prompt_builder.py` | Assembles the full instruction payload sent to Claude |
| `lib/nl2sql_pipeline.py` | Orchestrates the entire pipeline, handles retries |
| `lib/sql_validator.py` | Checks whether the generated SQL is syntactically valid |
| `lib/ddl_parser.py` | Parses CREATE TABLE statements from pasted DDL text |

---

## 3. What is an embedding?

An **embedding** is a way to represent text as a fixed-size list of numbers (a vector). The key property is that texts with similar *meaning* produce vectors that are numerically close to each other, even if the words are different.

### Concrete example

```
"employees"         → [0.12, -0.45, 0.87, 0.03, ...]  (384 numbers)
"staff headcount"   → [0.11, -0.43, 0.85, 0.04, ...]  (very similar)
"product inventory" → [0.89,  0.22, -0.31, 0.67, ...]  (very different)
```

You cannot read meaning from these numbers directly — they are learned representations from training on billions of text examples. What matters is the *distance* between vectors.

### The model we use

We use `all-MiniLM-L6-v2` from the `sentence-transformers` library. It:
- Runs **locally on CPU** — no API key or external call needed
- Produces **384-dimensional vectors** (a list of 384 floats)
- Is ~90 MB and downloads automatically on first use
- Takes ~5–20 ms per text on a modern CPU

```python
# lib/embeddings.py
from sentence_transformers import SentenceTransformer

_model = SentenceTransformer("all-MiniLM-L6-v2")

def embed_one(text: str) -> list[float]:
    return _model.encode([text], convert_to_numpy=True)[0].tolist()
```

### When embeddings are created

Every table name and column signature is embedded **at import time** and stored in the database:

```
"orders"                           → vector stored in schema_tables.embedding
"orders.customer_id (UUID)"        → vector stored in schema_columns.embedding
"orders.total_amount (NUMERIC)"    → vector stored in schema_columns.embedding
```

When a user submits a query, only that query text is embedded in real time (one fast local call).

---

## 4. Vector search with pgvector

**pgvector** is a Postgres extension that adds a `VECTOR` column type and vector distance operators. It lets you run nearest-neighbour searches inside the database using normal SQL.

### Cosine similarity

The distance metric used is **cosine similarity** — it measures the angle between two vectors rather than their absolute distance. This is robust to variations in phrasing length.

- Cosine similarity of **1.0** = identical meaning
- Cosine similarity of **0.0** = completely unrelated
- Cosine similarity of **-1.0** = opposite meaning

In pgvector, the `<=>` operator computes cosine distance (1 − similarity), so `ORDER BY embedding <=> query_vec` returns the most similar rows first.

### Example query

```sql
-- Find the 15 tables most relevant to the user's question
SELECT id, name
FROM schema_tables
WHERE schema_id = '...'
ORDER BY embedding <=> '[0.12, -0.45, 0.87, ...]'::vector
LIMIT 15;
```

This is a standard SQL query — the vector search is just an `ORDER BY` with a special operator.

### HNSW index

Without an index, finding the nearest vector requires comparing against every row (a full table scan). For large tables this is slow.

We use an **HNSW index** (Hierarchical Navigable Small World), a graph-based approximate nearest-neighbour index that trades a small amount of accuracy for a very large speed gain.

```sql
CREATE INDEX ON schema_tables
USING hnsw (embedding vector_cosine_ops);
```

Think of it like a B-tree index, but for multi-dimensional space instead of sorted scalar values.

---

## 5. Schema pruning

### The problem

A language model has a **context window** — a maximum amount of text it can process in one request (roughly analogous to RAM for a program). Sending a large database schema with hundreds of tables would:

1. Exceed or approach the context limit
2. Waste tokens (which cost money and slow down responses)
3. Reduce accuracy — more irrelevant information makes the model less focused

### The solution

Instead of sending the full schema, we use the embedding of the user's question to find only the tables most likely to be needed for that specific query.

```python
# lib/schema_pruner.py
def get_relevant_tables(schema_id: str, query: str, top_k: int = 15):
    query_vec = embed_one(query)           # embed the question
    # ask pgvector for the 15 closest table embeddings
    cur.execute("""
        SELECT id, name, description
        FROM schema_tables
        WHERE schema_id = %s
        ORDER BY embedding <=> %s::vector
        LIMIT %s
    """, (schema_id, query_vec, top_k))
    return cur.fetchall()
```

For a question like *"Show top customers by revenue"*, this returns tables like `customers`, `orders`, `order_items` — and leaves out unrelated tables like `performance_reviews` or `job_titles`.

The pruned table list (with all their columns) is then rendered as DDL and injected into the prompt Claude receives.

---

## 6. Few-shot examples

### What is few-shot learning?

Language models learn from context. If you show the model a few examples of the pattern you want, it performs significantly better than if you describe the pattern in words alone. This is called **few-shot prompting**.

In our case, each schema has curated example pairs:

```
Q: How many customers do we have in total?
SQL:
SELECT COUNT(*) AS total_customers
FROM customers;

Q: Which products are out of stock?
SQL:
SELECT name, sku
FROM products
WHERE stock_quantity = 0;
```

These examples teach Claude:
- The exact table and column names in this schema
- The expected output format (no SELECT *, use aliases, etc.)
- The complexity level and style expected

### Retrieval

We do not inject all examples every time — that would waste context. Instead we retrieve only the **3 most semantically similar examples** to the current question using the same pgvector cosine search approach used for tables.

```sql
SELECT nl_query, sql_query
FROM few_shot_examples
WHERE schema_id = %s
ORDER BY embedding <=> %s::vector
LIMIT 3;
```

If a user asks about revenue, the top-3 examples retrieved will be the ones most related to revenue queries, not schema setup questions.

---

## 7. What is an LLM and how does Claude work?

### What is an LLM?

A **Large Language Model (LLM)** is a neural network trained to predict the next token (roughly: next word or word-piece) in a sequence of text. Through training on a very large corpus of text, the model learns statistical patterns about language, reasoning, and code.

At inference time (when you use it), you provide a prompt — a sequence of text — and the model generates a continuation, one token at a time.

**Claude** is Anthropic's LLM. We use `claude-sonnet-4-6`, which is a balanced model for quality and cost.

### Tokens

A **token** is the unit of text the model processes. Roughly:
- 1 token ≈ 0.75 English words
- `"SELECT * FROM orders WHERE"` is about 7 tokens
- The model is billed per input token (what you send) + per output token (what it generates)

### The context window

Claude has a context window of ~200,000 tokens. Everything — the system prompt, the schema DDL, the examples, the user question, and the generated response — must fit within this limit. This is why schema pruning matters: a large schema DDL can be tens of thousands of tokens.

We set `max_tokens=4096` for the response. This caps the output length. Complex queries with long reasoning chains need room to think before writing SQL, which is why 4096 is necessary (2048 was insufficient for cross-schema queries).

---

## 8. Prompt engineering

**Prompt engineering** is the practice of structuring the text you send to an LLM to get the output you want. It is analogous to writing a clear function contract or API specification.

### Our system prompt structure

The system prompt is sent alongside every request and sets the context for the model. Ours contains four layers:

```
┌─────────────────────────────────────────────┐
│  1. Role definition                          │
│     "You are an expert SQL engineer          │
│      specializing in POSTGRESQL."            │
├─────────────────────────────────────────────┤
│  2. Dialect-specific SQL rules               │
│     "- Use ILIKE for case-insensitive        │
│        matching                              │
│      - Use EXTRACT(epoch FROM ...) for       │
│        timestamp arithmetic"                 │
├─────────────────────────────────────────────┤
│  3. Schema DDL (pruned to relevant tables)   │
│     CREATE TABLE customers (                 │
│       id UUID PRIMARY KEY,                   │
│       email TEXT NOT NULL,                   │
│       ...                                    │
│     );                                       │
├─────────────────────────────────────────────┤
│  4. Few-shot examples (top 3 by similarity)  │
│     Q: How many customers do we have?        │
│     SQL: SELECT COUNT(*) FROM customers;     │
└─────────────────────────────────────────────┘
```

The user's natural language question is sent separately as the "user" message.

### Output format constraints

We instruct Claude to wrap its output in XML-style tags:

```
<thinking>
Step-by-step reasoning about which tables and joins are needed...
</thinking>
<sql>
SELECT ...
FROM ...
</sql>
```

The `<thinking>` block lets the model reason before committing to SQL — this is a technique called **chain-of-thought prompting** and measurably improves accuracy on complex queries. The `<sql>` tags let us reliably extract just the SQL from the response using a simple regex.

---

## 9. Prompt caching

### The cost problem

Every API request bills for all the tokens in the input — including the system prompt. For a large schema, the system prompt alone might be 3,000–10,000 tokens. If 100 users each ask 5 questions, that is 500 full system prompt transmissions, each with thousands of tokens.

### How caching works

Anthropic supports **prompt caching** via a `cache_control` flag on the system prompt:

```python
client.messages.stream(
    system=[{
        "type": "text",
        "text": system_prompt,
        "cache_control": {"type": "ephemeral"}  # mark for caching
    }],
    messages=[{"role": "user", "content": nl_query}],
)
```

When the same system prompt is sent again within a 5-minute window, Anthropic serves it from cache. **Cached tokens cost ~90% less than regular input tokens.**

In practice: the first query from a user pays full price. Every subsequent query in the same session that uses the same schema pays only for the user's question tokens, not the schema DDL. For heavy schemas this is roughly a 5–10× cost reduction.

---

## 10. Streaming

### What is streaming?

Instead of waiting for Claude to generate the entire response and then sending it all at once, the API can send tokens as they are generated — similar to a chunked HTTP response or a WebSocket stream. The user sees SQL appearing character by character rather than waiting 5–10 seconds for a blank screen.

### How it works in the app

We open a streaming context and iterate over tokens:

```python
with client.messages.stream(...) as stream:
    for token in stream.text_stream:
        raw_buf += token
        # Look for the opening and closing <sql> tags in what we have so far
        s0 = raw_buf.find("<sql>")
        s1 = raw_buf.find("</sql>")
        if s0 >= 0:
            sql_text = raw_buf[s0 + 5 : s1 if s1 >= 0 else len(raw_buf)]
            if sql_text.strip():
                sql_ph.code(sql_text.strip(), language="sql")  # update UI
```

`sql_ph` is a Streamlit `st.empty()` placeholder — a UI slot that can be replaced in place. Each time we have more SQL content, we replace the placeholder's content with the updated SQL. From the user's perspective, the SQL grows progressively on screen.

The `<thinking>` block arrives first (before `<sql>`), so the SQL placeholder shows "Generating..." while Claude is reasoning. Once the `<sql>` tag appears, real SQL starts streaming into the UI.

---

## 11. SQL validation and error recovery

### Validation

Once streaming completes, the extracted SQL is passed through `sqlparse`:

```python
import sqlparse

def validate_and_format(sql: str) -> tuple[bool, str, str | None]:
    statements = sqlparse.parse(sql)
    if not statements or not any(s.tokens for s in statements):
        return False, sql, "Could not parse SQL into any statements"
    formatted = sqlparse.format(sql, reindent=True, keyword_case="upper")
    return True, formatted, None
```

`sqlparse` is a pure-Python SQL parser. It checks that the SQL is syntactically well-formed and also normalises it (uppercases keywords, adds consistent indentation).

### Execution safety

Before running any SQL against the real database, we check that it is a read-only query:

```python
first_keyword = sql.strip().split()[0].upper()
if first_keyword in {"INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "TRUNCATE"}:
    return None, "Only SELECT queries are executed automatically."
```

`WITH ... SELECT` (CTEs) are allowed because their first keyword is `WITH`, not a write keyword.

### Error recovery (retry loop)

If validation fails, we do not just show an error. Instead, we feed the error back to Claude in a follow-up message:

```python
for attempt in range(3):
    raw = call_llm(system_prompt, messages)
    is_valid, formatted_sql, error = validate_and_format(raw)

    if is_valid:
        return formatted_sql

    # Inject the error as a follow-up and try again
    messages = [
        {"role": "user",    "content": original_question},
        {"role": "assistant","content": f"<sql>{bad_sql}</sql>"},
        {"role": "user",    "content": f"That SQL has a parse error: {error}\n"
                                       f"Please fix it and output only the corrected SQL."},
    ]
```

This is a **multi-turn conversation** with the model. Claude sees its own previous (broken) output and the specific error message, which gives it enough context to self-correct. In practice, one retry resolves almost all validation failures.

---

## 12. Cross-schema mode

### The challenge

When a user wants to ask a question that spans two schemas (e.g. compare revenue from the E-commerce schema with headcount from the HR schema), running the pipeline independently for each schema does not work — each Claude call only sees one schema's tables and cannot generate SQL that joins across both.

### The solution: DDL merging

In cross-schema mode, the pruning step runs for each schema separately, then the results are merged before building the prompt:

```python
all_tables = []
all_columns_by_table = {}

for schema in selected_schemas:
    ctx = get_schema_context(schema["id"], nl_query)
    all_tables.extend(ctx["tables"])              # append tables from each schema
    all_columns_by_table.update(ctx["columns_by_table"])

# Now render one DDL block containing tables from all schemas
schema_ddl = render_schema_ddl(all_tables, all_columns_by_table)
```

The combined DDL is sent in a single Claude call. The system prompt names the database as `"E-commerce + HR"` and Claude sees all relevant tables at once. Because all tables physically live in the same Neon Postgres database (they just have separate metadata entries in `registered_schemas`), the generated SQL can legitimately JOIN across them.

---

## 13. The complete request lifecycle

Here is what happens when a user types a question and clicks **Generate SQL**:

```
1. User submits:  "Compare revenue growth with headcount changes last 6 months"

2. Embed query:   embed_one(query)  → 384-dim vector  [~10 ms, local CPU]

3. Schema prune:  pgvector cosine search against schema_tables embeddings
                  Returns: [orders, customers, order_items, employees, departments]
                  [~5 ms, single SQL query against Neon]

4. Column fetch:  SELECT all columns WHERE table_id IN (pruned table ids)
                  [~5 ms]

5. Few-shot:      pgvector cosine search against few_shot_examples embeddings
                  Returns top 3 NL→SQL pairs most similar to this question
                  [~5 ms]

6. Build prompt:  Assemble system prompt:
                    role + rules + schema DDL + 3 examples
                  Total: ~2,000–8,000 tokens depending on schema size

7. Claude call:   POST to Anthropic API with system + user message
                  Streaming response begins arriving within ~1 second
                  Model generates <thinking> block first (~500–1000 tokens)
                  Then <sql> block (~100–500 tokens)
                  Total latency: ~5–15 seconds for complex queries

8. Streaming UI:  As <sql> tokens arrive, Streamlit placeholder updates live
                  Reasoning appears in right panel after stream completes

9. Validate:      sqlparse checks the extracted SQL
                  If invalid: retry loop (max 2 retries with error feedback)

10. Execute:      Run validated SQL against Neon Postgres
                  Fetch up to 300 rows
                  Render as a dataframe in the UI

11. Log:          INSERT into query_history (nl_query, generated_sql, retries)
```

---

## 14. Data flow diagram

```
User browser
    │
    │  NL query text
    ▼
Streamlit (app.py)
    │
    ├─► embed_one(query) ──────────────────► sentence-transformers (local)
    │       │ 384-dim vector
    │       ▼
    ├─► pgvector cosine search ────────────► Neon Postgres
    │       │ top-15 tables + columns         (schema_tables, schema_columns,
    │       │ top-3 few-shot examples          few_shot_examples tables)
    │       ▼
    ├─► render_schema_ddl()
    │       │ DDL string
    │       ▼
    ├─► build_system_prompt()
    │       │ Full prompt (role + rules + DDL + examples)
    │       ▼
    ├─► Anthropic API (streaming) ─────────► Claude claude-sonnet-4-6
    │       │ token stream                    generates <thinking> + <sql>
    │       ▼
    ├─► extract <sql> from stream
    │       │ raw SQL string
    │       ▼
    ├─► sqlparse validate + format
    │       │ (retry loop if invalid)
    │       ▼
    ├─► execute_sql() ─────────────────────► Neon Postgres
    │       │ rows (max 300)                  (actual data tables)
    │       ▼
    └─► render DataFrame + SQL in UI
```

---

## Glossary

| Term | Plain English meaning |
|---|---|
| **Token** | The smallest unit of text an LLM processes (~0.75 words) |
| **Context window** | The maximum total text (input + output) an LLM can handle per request |
| **Embedding** | A fixed-size list of numbers representing the semantic meaning of text |
| **Vector** | A list of numbers — in this codebase, always a 384-dim embedding |
| **Cosine similarity** | A measure of how similar two vectors are based on the angle between them |
| **pgvector** | A Postgres extension that stores vectors and runs nearest-neighbour search |
| **HNSW** | A graph index for fast approximate nearest-neighbour search in vector space |
| **Schema pruning** | Selecting only the most relevant tables from a large schema before sending to LLM |
| **Few-shot prompting** | Providing example input/output pairs in the prompt to guide model behaviour |
| **Chain-of-thought** | Asking the model to reason step by step before giving a final answer |
| **Prompt caching** | Reusing a previously computed system prompt to reduce token cost |
| **Streaming** | Receiving LLM output token by token as it is generated, rather than all at once |
| **CTE** | Common Table Expression — a SQL `WITH` clause that defines a named subquery |
| **Retry loop** | Sending a failed SQL + its error message back to the LLM and asking it to fix the output |
