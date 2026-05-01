"""
Simple regex-based DDL parser.
Handles common CREATE TABLE patterns without needing a full SQL parser.
"""
import re
from dataclasses import dataclass, field


@dataclass
class ColumnDef:
    name: str
    data_type: str
    is_nullable: bool = True
    is_primary_key: bool = False
    is_foreign_key: bool = False
    fk_references: str | None = None


@dataclass
class TableDef:
    name: str
    columns: list[ColumnDef] = field(default_factory=list)


def parse_ddl(ddl: str) -> list[TableDef]:
    """Parse a DDL string into a list of TableDef objects."""
    tables = []

    # Find all CREATE TABLE blocks
    pattern = re.compile(
        r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?["`]?(\w+)["`]?\s*\((.+?)\)\s*;',
        re.IGNORECASE | re.DOTALL,
    )

    for match in pattern.finditer(ddl):
        table_name = match.group(1)
        body = match.group(2)
        columns = _parse_columns(body)
        tables.append(TableDef(name=table_name, columns=columns))

    return tables


def _parse_columns(body: str) -> list[ColumnDef]:
    columns = []
    # Split by comma, but not commas inside parentheses (e.g. DECIMAL(10,2))
    lines = _split_columns(body)

    for line in lines:
        line = line.strip()
        if not line:
            continue

        upper = line.upper()
        # Skip table-level constraints
        if re.match(r'(PRIMARY\s+KEY|UNIQUE|CHECK|FOREIGN\s+KEY|INDEX|KEY)\s*\(', upper):
            continue

        col = _parse_column_line(line)
        if col:
            columns.append(col)

    return columns


def _split_columns(body: str) -> list[str]:
    """Split column definitions by comma, ignoring commas inside parentheses."""
    parts = []
    depth = 0
    current = []
    for ch in body:
        if ch == '(':
            depth += 1
            current.append(ch)
        elif ch == ')':
            depth -= 1
            current.append(ch)
        elif ch == ',' and depth == 0:
            parts.append(''.join(current))
            current = []
        else:
            current.append(ch)
    if current:
        parts.append(''.join(current))
    return parts


def _parse_column_line(line: str) -> ColumnDef | None:
    # Match: name type [constraints...]
    m = re.match(r'["`]?(\w+)["`]?\s+(\w+(?:\s*\([^)]*\))?)', line)
    if not m:
        return None

    name = m.group(1)
    data_type = m.group(2).upper()
    upper = line.upper()

    is_pk = bool(re.search(r'\bPRIMARY\s+KEY\b', upper))
    is_nullable = not (is_pk or bool(re.search(r'\bNOT\s+NULL\b', upper)))
    is_fk = bool(re.search(r'\bREFERENCES\b', upper))
    fk_ref = None
    if is_fk:
        fk_m = re.search(r'REFERENCES\s+["`]?(\w+)["`]?\s*\(\s*["`]?(\w+)["`]?\s*\)', line, re.IGNORECASE)
        if fk_m:
            fk_ref = f"{fk_m.group(1)}.{fk_m.group(2)}"

    return ColumnDef(
        name=name,
        data_type=data_type,
        is_nullable=is_nullable,
        is_primary_key=is_pk,
        is_foreign_key=is_fk,
        fk_references=fk_ref,
    )
