"""
Seeds two demo schemas (E-commerce and HR) with tables, columns, embeddings,
and few-shot examples into Neon.

Usage: python scripts/seed_schemas.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import psycopg2
from dotenv import load_dotenv
from lib.embeddings import embed

load_dotenv()

# ---------------------------------------------------------------------------
# Schema definitions
# ---------------------------------------------------------------------------

ECOMMERCE_TABLES = [
    {
        "name": "customers",
        "description": "Registered customer accounts",
        "columns": [
            ("id", "UUID", False, True, False, None, "Primary key"),
            ("email", "TEXT", False, False, False, None, "Unique email address"),
            ("full_name", "TEXT", False, False, False, None, "Customer display name"),
            ("phone", "TEXT", True, False, False, None, "Optional phone number"),
            ("created_at", "TIMESTAMPTZ", False, False, False, None, "Account creation timestamp"),
        ],
    },
    {
        "name": "categories",
        "description": "Product category hierarchy",
        "columns": [
            ("id", "UUID", False, True, False, None, "Primary key"),
            ("name", "TEXT", False, False, False, None, "Category name (e.g. Electronics)"),
            ("parent_id", "UUID", True, False, True, "categories.id", "Parent category for nested hierarchy"),
        ],
    },
    {
        "name": "products",
        "description": "Product catalog",
        "columns": [
            ("id", "UUID", False, True, False, None, "Primary key"),
            ("name", "TEXT", False, False, False, None, "Product name"),
            ("description", "TEXT", True, False, False, None, "Product description"),
            ("price", "NUMERIC(10,2)", False, False, False, None, "Unit price in USD"),
            ("category_id", "UUID", True, False, True, "categories.id", "Product category"),
            ("stock_qty", "INTEGER", False, False, False, None, "Units in stock"),
            ("is_active", "BOOLEAN", False, False, False, None, "Whether the product is listed"),
        ],
    },
    {
        "name": "addresses",
        "description": "Customer shipping and billing addresses",
        "columns": [
            ("id", "UUID", False, True, False, None, "Primary key"),
            ("customer_id", "UUID", False, False, True, "customers.id", "Address owner"),
            ("street", "TEXT", False, False, False, None, "Street address line"),
            ("city", "TEXT", False, False, False, None, "City"),
            ("state", "TEXT", True, False, False, None, "State or province"),
            ("country", "TEXT", False, False, False, None, "Country code (ISO 3166-1)"),
            ("postal_code", "TEXT", True, False, False, None, "ZIP or postal code"),
            ("is_default", "BOOLEAN", False, False, False, None, "Whether this is the default address"),
        ],
    },
    {
        "name": "orders",
        "description": "Customer orders",
        "columns": [
            ("id", "UUID", False, True, False, None, "Primary key"),
            ("customer_id", "UUID", False, False, True, "customers.id", "Order owner"),
            ("status", "TEXT", False, False, False, None, "Order status: pending, paid, shipped, delivered, cancelled"),
            ("total_amount", "NUMERIC(10,2)", False, False, False, None, "Total order value in USD"),
            ("shipping_address_id", "UUID", True, False, True, "addresses.id", "Delivery address"),
            ("created_at", "TIMESTAMPTZ", False, False, False, None, "Order placement timestamp"),
            ("updated_at", "TIMESTAMPTZ", False, False, False, None, "Last status update timestamp"),
        ],
    },
    {
        "name": "order_items",
        "description": "Individual line items within an order",
        "columns": [
            ("id", "UUID", False, True, False, None, "Primary key"),
            ("order_id", "UUID", False, False, True, "orders.id", "Parent order"),
            ("product_id", "UUID", False, False, True, "products.id", "Product ordered"),
            ("quantity", "INTEGER", False, False, False, None, "Number of units ordered"),
            ("unit_price", "NUMERIC(10,2)", False, False, False, None, "Price per unit at time of purchase"),
        ],
    },
]

ECOMMERCE_EXAMPLES = [
    (
        "Show me the top 10 customers by total order value",
        """SELECT
    c.full_name,
    c.email,
    SUM(o.total_amount) AS total_spent,
    COUNT(o.id) AS order_count
FROM customers c
JOIN orders o ON o.customer_id = c.id
WHERE o.status != 'cancelled'
GROUP BY c.id, c.full_name, c.email
ORDER BY total_spent DESC
LIMIT 10;""",
    ),
    (
        "List all out-of-stock active products with their category names",
        """SELECT
    p.name AS product_name,
    p.price,
    cat.name AS category
FROM products p
JOIN categories cat ON cat.id = p.category_id
WHERE p.stock_qty = 0
  AND p.is_active = TRUE
ORDER BY cat.name, p.name;""",
    ),
    (
        "What is the monthly revenue for the past 6 months?",
        """SELECT
    DATE_TRUNC('month', o.created_at) AS month,
    SUM(o.total_amount) AS revenue,
    COUNT(o.id) AS order_count
FROM orders o
WHERE o.status != 'cancelled'
  AND o.created_at >= NOW() - INTERVAL '6 months'
GROUP BY month
ORDER BY month DESC;""",
    ),
    (
        "Find customers who placed an order but have not ordered in the last 90 days",
        """SELECT DISTINCT
    c.full_name,
    c.email,
    MAX(o.created_at) AS last_order_date
FROM customers c
JOIN orders o ON o.customer_id = c.id
GROUP BY c.id, c.full_name, c.email
HAVING MAX(o.created_at) < NOW() - INTERVAL '90 days'
ORDER BY last_order_date ASC;""",
    ),
    (
        "What are the 5 best-selling products by quantity sold this year?",
        """SELECT
    p.name AS product_name,
    SUM(oi.quantity) AS units_sold,
    SUM(oi.quantity * oi.unit_price) AS revenue
FROM order_items oi
JOIN products p ON p.id = oi.product_id
JOIN orders o ON o.id = oi.order_id
WHERE o.status != 'cancelled'
  AND EXTRACT(YEAR FROM o.created_at) = EXTRACT(YEAR FROM NOW())
GROUP BY p.id, p.name
ORDER BY units_sold DESC
LIMIT 5;""",
    ),
]

# ---------------------------------------------------------------------------

HR_TABLES = [
    {
        "name": "departments",
        "description": "Company departments",
        "columns": [
            ("id", "UUID", False, True, False, None, "Primary key"),
            ("name", "TEXT", False, False, False, None, "Department name (e.g. Engineering)"),
            ("budget", "NUMERIC(15,2)", True, False, False, None, "Annual budget in USD"),
            ("location", "TEXT", True, False, False, None, "Office location or remote"),
        ],
    },
    {
        "name": "job_titles",
        "description": "Job title definitions with salary bands",
        "columns": [
            ("id", "UUID", False, True, False, None, "Primary key"),
            ("title", "TEXT", False, False, False, None, "Job title (e.g. Senior Engineer)"),
            ("min_salary", "NUMERIC(12,2)", True, False, False, None, "Salary band minimum"),
            ("max_salary", "NUMERIC(12,2)", True, False, False, None, "Salary band maximum"),
        ],
    },
    {
        "name": "employees",
        "description": "Employee records",
        "columns": [
            ("id", "UUID", False, True, False, None, "Primary key"),
            ("first_name", "TEXT", False, False, False, None, "Given name"),
            ("last_name", "TEXT", False, False, False, None, "Family name"),
            ("email", "TEXT", False, False, False, None, "Work email address"),
            ("hire_date", "DATE", False, False, False, None, "Date employee joined the company"),
            ("department_id", "UUID", True, False, True, "departments.id", "Employee's department"),
            ("job_title_id", "UUID", True, False, True, "job_titles.id", "Current job title"),
            ("manager_id", "UUID", True, False, True, "employees.id", "Direct manager (self-reference)"),
            ("is_active", "BOOLEAN", False, False, False, None, "Whether currently employed"),
        ],
    },
    {
        "name": "salaries",
        "description": "Historical salary records per employee",
        "columns": [
            ("id", "UUID", False, True, False, None, "Primary key"),
            ("employee_id", "UUID", False, False, True, "employees.id", "Salary recipient"),
            ("amount", "NUMERIC(12,2)", False, False, False, None, "Annual salary in USD"),
            ("effective_date", "DATE", False, False, False, None, "Date this salary took effect"),
            ("end_date", "DATE", True, False, False, None, "Date this salary ended (NULL = current)"),
        ],
    },
    {
        "name": "performance_reviews",
        "description": "Annual performance review records",
        "columns": [
            ("id", "UUID", False, True, False, None, "Primary key"),
            ("employee_id", "UUID", False, False, True, "employees.id", "Reviewed employee"),
            ("reviewer_id", "UUID", False, False, True, "employees.id", "Reviewing manager"),
            ("rating", "INTEGER", False, False, False, None, "Performance rating 1-5"),
            ("comments", "TEXT", True, False, False, None, "Qualitative feedback"),
            ("review_date", "DATE", False, False, False, None, "Date of the review"),
        ],
    },
    {
        "name": "time_off",
        "description": "Employee time-off requests",
        "columns": [
            ("id", "UUID", False, True, False, None, "Primary key"),
            ("employee_id", "UUID", False, False, True, "employees.id", "Requesting employee"),
            ("type", "TEXT", False, False, False, None, "Leave type: vacation, sick, parental, unpaid"),
            ("start_date", "DATE", False, False, False, None, "First day of leave"),
            ("end_date", "DATE", False, False, False, None, "Last day of leave"),
            ("status", "TEXT", False, False, False, None, "Status: pending, approved, rejected"),
        ],
    },
]

HR_EXAMPLES = [
    (
        "List all employees in Engineering with their current salary",
        """SELECT
    e.first_name,
    e.last_name,
    e.email,
    s.amount AS current_salary
FROM employees e
JOIN departments d ON d.id = e.department_id
JOIN salaries s ON s.employee_id = e.id AND s.end_date IS NULL
WHERE d.name = 'Engineering'
  AND e.is_active = TRUE
ORDER BY e.last_name;""",
    ),
    (
        "Who are the managers and how many direct reports does each have?",
        """SELECT
    m.first_name || ' ' || m.last_name AS manager_name,
    COUNT(e.id) AS direct_reports
FROM employees e
JOIN employees m ON m.id = e.manager_id
WHERE e.is_active = TRUE
GROUP BY m.id, m.first_name, m.last_name
ORDER BY direct_reports DESC;""",
    ),
    (
        "What is the average current salary by department?",
        """SELECT
    d.name AS department,
    ROUND(AVG(s.amount), 2) AS avg_salary,
    COUNT(e.id) AS headcount
FROM employees e
JOIN departments d ON d.id = e.department_id
JOIN salaries s ON s.employee_id = e.id AND s.end_date IS NULL
WHERE e.is_active = TRUE
GROUP BY d.id, d.name
ORDER BY avg_salary DESC;""",
    ),
    (
        "Show employees who received a performance rating of 5 in their most recent review",
        """WITH latest_reviews AS (
    SELECT
        employee_id,
        rating,
        review_date,
        ROW_NUMBER() OVER (PARTITION BY employee_id ORDER BY review_date DESC) AS rn
    FROM performance_reviews
)
SELECT
    e.first_name,
    e.last_name,
    e.email,
    lr.rating,
    lr.review_date
FROM employees e
JOIN latest_reviews lr ON lr.employee_id = e.id AND lr.rn = 1
WHERE lr.rating = 5
  AND e.is_active = TRUE
ORDER BY lr.review_date DESC;""",
    ),
    (
        "Find employees who have been with the company for more than 5 years",
        """SELECT
    e.first_name,
    e.last_name,
    e.hire_date,
    EXTRACT(YEAR FROM AGE(NOW(), e.hire_date)) AS years_at_company,
    d.name AS department
FROM employees e
JOIN departments d ON d.id = e.department_id
WHERE e.is_active = TRUE
  AND e.hire_date <= NOW() - INTERVAL '5 years'
ORDER BY e.hire_date ASC;""",
    ),
]

# ---------------------------------------------------------------------------
# Insertion helpers
# ---------------------------------------------------------------------------


def insert_schema(cur, name: str, description: str, is_demo: bool = True) -> str:
    cur.execute(
        "INSERT INTO registered_schemas (name, description, is_demo) VALUES (%s, %s, %s) RETURNING id",
        (name, description, is_demo),
    )
    return cur.fetchone()[0]


def insert_tables_and_columns(cur, schema_id: str, tables: list[dict]) -> list[tuple[str, str]]:
    """Insert tables and columns; return list of (text_for_embedding, table_id) pairs."""
    table_embed_inputs = []  # (text, table_id)
    col_embed_inputs = []    # (text, col_id)

    for tdef in tables:
        cur.execute(
            "INSERT INTO schema_tables (schema_id, name, description) VALUES (%s, %s, %s) RETURNING id",
            (schema_id, tdef["name"], tdef["description"]),
        )
        table_id = cur.fetchone()[0]
        embed_text = f"{tdef['name']}: {tdef['description']}"
        table_embed_inputs.append((embed_text, table_id))

        for col in tdef["columns"]:
            cname, ctype, nullable, pk, fk, fk_ref, cdesc = col
            cur.execute(
                """INSERT INTO schema_columns
                    (table_id, schema_id, name, data_type, is_nullable, is_primary_key,
                     is_foreign_key, fk_references, description)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id""",
                (table_id, schema_id, cname, ctype, nullable, pk, fk, fk_ref, cdesc),
            )
            col_id = cur.fetchone()[0]
            col_embed_text = f"{tdef['name']}.{cname} ({ctype}): {cdesc}"
            col_embed_inputs.append((col_embed_text, col_id))

    return table_embed_inputs, col_embed_inputs


def apply_embeddings(cur, table_inputs: list, col_inputs: list):
    if table_inputs:
        texts = [t for t, _ in table_inputs]
        vecs = embed(texts)
        for (_, tid), vec in zip(table_inputs, vecs):
            cur.execute(
                "UPDATE schema_tables SET embedding = %s::vector WHERE id = %s",
                (vec, tid),
            )

    if col_inputs:
        texts = [t for t, _ in col_inputs]
        vecs = embed(texts)
        for (_, cid), vec in zip(col_inputs, vecs):
            cur.execute(
                "UPDATE schema_columns SET embedding = %s::vector WHERE id = %s",
                (vec, cid),
            )


def insert_examples(cur, schema_id: str, examples: list[tuple[str, str]]):
    ex_ids = []
    for nl, sql in examples:
        cur.execute(
            "INSERT INTO few_shot_examples (schema_id, nl_query, sql_query) VALUES (%s, %s, %s) RETURNING id",
            (schema_id, nl, sql),
        )
        ex_ids.append((nl, cur.fetchone()[0]))

    texts = [nl for nl, _ in ex_ids]
    vecs = embed(texts)
    for (_, eid), vec in zip(ex_ids, vecs):
        cur.execute(
            "UPDATE few_shot_examples SET embedding = %s::vector WHERE id = %s",
            (vec, eid),
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    try:
        with conn.cursor() as cur:
            # Delete existing demo schemas to allow re-seeding
            cur.execute("DELETE FROM registered_schemas WHERE is_demo = TRUE")

            print("Seeding E-commerce schema...")
            ec_id = insert_schema(cur, "E-commerce", "Online retail platform with customers, products, orders, and inventory")
            table_in, col_in = insert_tables_and_columns(cur, ec_id, ECOMMERCE_TABLES)
            apply_embeddings(cur, table_in, col_in)
            insert_examples(cur, ec_id, ECOMMERCE_EXAMPLES)
            print(f"  E-commerce schema id: {ec_id}")

            print("Seeding HR schema...")
            hr_id = insert_schema(cur, "HR", "Human resources system with employees, departments, salaries, and performance reviews")
            table_in, col_in = insert_tables_and_columns(cur, hr_id, HR_TABLES)
            apply_embeddings(cur, table_in, col_in)
            insert_examples(cur, hr_id, HR_EXAMPLES)
            print(f"  HR schema id: {hr_id}")

        conn.commit()
        print("Done. Demo schemas seeded successfully.")
    finally:
        conn.close()


if __name__ == "__main__":
    import psycopg2
    main()
