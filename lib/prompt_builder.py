DIALECT_RULES: dict[str, str] = {
    "postgresql": (
        "- Use ILIKE for case-insensitive matching\n"
        "- Use EXTRACT(epoch FROM ...) for timestamp arithmetic\n"
        "- Use array_agg() to aggregate into arrays\n"
        "- Quote identifiers with double quotes only when necessary"
    ),
    "mysql": (
        "- Use backticks for identifiers\n"
        "- Use GROUP_CONCAT() instead of array_agg()\n"
        "- Use LOWER() + LIKE for case-insensitive matching (no ILIKE)\n"
        "- Use LIMIT n for row limits"
    ),
    "bigquery": (
        "- Use backtick-qualified names: `project.dataset.table`\n"
        "- Use UNNEST() for array columns\n"
        "- Use DATE_TRUNC() for date truncation\n"
        "- Use COUNTIF() for conditional counting"
    ),
    "snowflake": (
        "- Use QUALIFY for window function row filtering\n"
        "- Use FLATTEN() for semi-structured/variant data\n"
        "- Use TO_TIMESTAMP() for timestamp conversion\n"
        "- Use ILIKE for case-insensitive matching"
    ),
}


def render_schema_ddl(tables: list[dict], columns_by_table: dict[str, list[dict]]) -> str:
    lines = []
    for table in tables:
        tid = str(table["id"])
        comment = f"  -- {table['description']}" if table.get("description") else ""
        lines.append(f"CREATE TABLE {table['name']} ({comment}")
        cols = columns_by_table.get(tid, [])
        col_defs = []
        for col in cols:
            parts = [f"  {col['name']} {col['data_type'].upper()}"]
            if col.get("is_primary_key"):
                parts.append("PRIMARY KEY")
            if not col.get("is_nullable"):
                parts.append("NOT NULL")
            if col.get("fk_references"):
                parts.append(f"REFERENCES {col['fk_references']}")
            col_def = " ".join(parts)
            if col.get("description"):
                col_def += f"  -- {col['description']}"
            col_defs.append(col_def)
        lines.append(",\n".join(col_defs))
        lines.append(");\n")
    return "\n".join(lines)


def build_system_prompt(schema_name: str, dialect: str, schema_ddl: str, examples: list[dict]) -> str:
    dialect_rules = DIALECT_RULES.get(dialect, DIALECT_RULES["postgresql"])
    prompt = f"""You are an expert SQL engineer specializing in {dialect.upper()}.

Convert the user's natural language question into a precise SQL query.

{dialect.upper()} rules:
{dialect_rules}

General rules:
- Never use SELECT *; list columns explicitly
- Use table aliases for readability
- Prefer CTEs over deeply nested subqueries

Output format (strict — always follow this):
1. Write your step-by-step reasoning inside <thinking>...</thinking>
2. Write ONLY the final SQL inside <sql>...</sql>

Database: {schema_name}
Schema:
{schema_ddl}"""

    if examples:
        ex_parts = [f"Q: {ex['nl_query']}\nSQL:\n{ex['sql_query']}" for ex in examples]
        prompt += "\n\nExamples:\n\n" + "\n\n".join(ex_parts)

    return prompt
