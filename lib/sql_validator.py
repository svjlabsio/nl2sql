import sqlparse


def validate_and_format(sql: str) -> tuple[bool, str, str | None]:
    """
    Returns (is_valid, formatted_sql, error_message).
    Uses sqlparse for formatting; validates that at least one statement parsed.
    """
    sql = sql.strip()
    if not sql:
        return False, sql, "Empty SQL generated"
    try:
        statements = sqlparse.parse(sql)
        if not statements or not any(s.tokens for s in statements):
            return False, sql, "Could not parse SQL into any statements"
        formatted = sqlparse.format(
            sql,
            reindent=True,
            keyword_case="upper",
            identifier_case=None,
            strip_comments=False,
        )
        return True, formatted, None
    except Exception as e:
        return False, sql, str(e)
