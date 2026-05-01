"""
Creates physical demo tables and inserts realistic sample data.
Run after init_db.py and seed_schemas.py.

Usage: python scripts/seed_data.py
"""
import os, sys, uuid, random
from datetime import date, timedelta, datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import psycopg2
from dotenv import load_dotenv

load_dotenv()
random.seed(42)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def gid(): return str(uuid.uuid4())

def rand_date(days_ago_far: int, days_ago_near: int) -> date:
    offset = random.randint(days_ago_near, days_ago_far)
    return date.today() - timedelta(days=offset)

def rand_ts(days_ago_far: int, days_ago_near: int) -> datetime:
    offset = random.randint(days_ago_near, days_ago_far)
    hour = random.randint(0, 23)
    minute = random.randint(0, 59)
    d = date.today() - timedelta(days=offset)
    return datetime(d.year, d.month, d.day, hour, minute, tzinfo=timezone.utc)

# ---------------------------------------------------------------------------
# Reference data
# ---------------------------------------------------------------------------

FIRST_NAMES = [
    "Alice","Bob","Carol","David","Emma","Frank","Grace","Henry",
    "Isabel","James","Karen","Liam","Maria","Noah","Olivia","Paul",
    "Quinn","Rachel","Samuel","Tina","Uma","Victor","Wendy","Xavier",
    "Yara","Zoe","Aaron","Bella","Carlos","Diana",
]
LAST_NAMES = [
    "Smith","Johnson","Williams","Brown","Jones","Garcia","Miller",
    "Davis","Rodriguez","Martinez","Hernandez","Lopez","Gonzalez",
    "Wilson","Anderson","Thomas","Taylor","Moore","Jackson","Martin",
    "Lee","Perez","Thompson","White","Harris","Sanchez","Clark",
    "Ramirez","Lewis","Robinson",
]
CITIES = [
    ("New York","NY"), ("Los Angeles","CA"), ("Chicago","IL"),
    ("Houston","TX"), ("Phoenix","AZ"), ("Philadelphia","PA"),
    ("San Antonio","TX"), ("San Diego","CA"), ("Dallas","TX"),
    ("San Jose","CA"), ("Austin","TX"), ("Jacksonville","FL"),
    ("Seattle","WA"), ("Denver","CO"), ("Boston","MA"),
]
STREETS = [
    "Main St","Oak Ave","Maple Dr","Cedar Ln","Pine Rd",
    "Elm St","Washington Blvd","Park Ave","Lake Dr","River Rd",
]

# ---------------------------------------------------------------------------
# E-commerce data
# ---------------------------------------------------------------------------

CATEGORIES = [
    ("Electronics",     None),
    ("Clothing",        None),
    ("Home & Kitchen",  None),
    ("Sports",          None),
    ("Books",           None),
    ("Beauty",          None),
    ("Toys",            None),
    ("Food & Grocery",  None),
]

PRODUCTS_BY_CAT = {
    "Electronics": [
        ("Wireless Headphones",    89.99,  50),
        ("USB-C Hub 7-Port",       45.99, 120),
        ("Mechanical Keyboard",   129.99,  35),
        ("27\" Monitor",          349.99,  20),
        ("Webcam 1080p",           59.99,  80),
    ],
    "Clothing": [
        ("Classic White Tee",      24.99, 200),
        ("Slim Fit Jeans",         59.99, 150),
        ("Running Jacket",         89.99,  75),
        ("Wool Socks 6-Pack",      19.99, 300),
        ("Leather Belt",           34.99,  90),
    ],
    "Home & Kitchen": [
        ("Pour-Over Coffee Maker", 49.99,  60),
        ("Cast Iron Skillet",      39.99,  45),
        ("Bamboo Cutting Board",   29.99,  90),
        ("French Press 1L",        34.99,  70),
        ("Knife Set 8-Piece",      79.99,  40),
    ],
    "Sports": [
        ("Yoga Mat",               39.99,  85),
        ("Resistance Bands Set",   24.99, 110),
        ("Foam Roller",            29.99,  60),
        ("Jump Rope",              14.99, 200),
        ("Water Bottle 32oz",      22.99, 180),
    ],
    "Books": [
        ("The Pragmatic Programmer",29.99, 100),
        ("Clean Code",             34.99,  90),
        ("Designing Data-Intensive Apps", 49.99, 70),
        ("Deep Work",              18.99, 130),
        ("Atomic Habits",          16.99, 150),
    ],
    "Beauty": [
        ("Vitamin C Serum",        28.99,  95),
        ("SPF 50 Sunscreen",       19.99, 140),
        ("Retinol Moisturizer",    35.99,  60),
        ("Micellar Water 400ml",   14.99, 120),
        ("Rose Hip Face Oil",      24.99,  75),
    ],
    "Toys": [
        ("LEGO Classic Set",       49.99,  55),
        ("Board Game — Catan",     44.99,  40),
        ("Remote Control Car",     39.99,  65),
        ("Jigsaw Puzzle 1000pc",   24.99,  80),
        ("Art Supply Kit",         29.99,  70),
    ],
    "Food & Grocery": [
        ("Organic Coffee Beans 1kg", 19.99, 160),
        ("Matcha Green Tea 100g",   22.99, 120),
        ("Dark Chocolate 85% 200g", 8.99, 250),
        ("Protein Powder Vanilla 2lb", 39.99, 90),
        ("Mixed Nuts 1kg",         24.99, 135),
    ],
}

ORDER_STATUSES = ["paid","shipped","delivered","delivered","delivered","cancelled","pending"]

# ---------------------------------------------------------------------------
# HR data
# ---------------------------------------------------------------------------

DEPARTMENTS = [
    ("Engineering",  2_500_000, "San Francisco, CA"),
    ("Sales",        1_800_000, "New York, NY"),
    ("Marketing",      900_000, "Austin, TX"),
    ("HR",             500_000, "Chicago, IL"),
    ("Finance",        700_000, "Boston, MA"),
]

JOB_TITLES = [
    ("Junior Software Engineer",  75_000,  105_000),
    ("Senior Software Engineer", 120_000,  170_000),
    ("Engineering Manager",      160_000,  220_000),
    ("Sales Representative",      55_000,   85_000),
    ("Sales Manager",             90_000,  130_000),
    ("Marketing Specialist",      60_000,   90_000),
    ("HR Generalist",             55_000,   80_000),
    ("Financial Analyst",         70_000,  100_000),
]

EMP_FIRST = ["Alex","Morgan","Jordan","Taylor","Casey","Riley","Drew","Jamie",
             "Avery","Blake","Charlie","Dakota","Emery","Finley","Hayden","Kendall",
             "Logan","Peyton","Reese","Sage","Skyler","Spencer","Toby","Wynne","Zara"]
EMP_LAST  = ["Chen","Park","Nguyen","Kim","Patel","Singh","Shah","Kumar",
             "Ali","Hassan","Ahmed","Santos","Oliveira","Fernandez","Costa",
             "Reed","Campbell","Mitchell","Perez","Roberts","Turner","Phillips"]

REVIEW_COMMENTS = [
    "Consistently exceeds expectations and delivers high-quality work.",
    "Good contributor, meets most targets with solid reliability.",
    "Shows great initiative and leadership potential.",
    "Needs improvement in communication and deadline management.",
    "Outstanding quarter — led two critical projects to completion.",
    "Reliable team player, contributes positively to team culture.",
    "Struggled with workload balance; coaching recommended.",
    "Exceptional analytical skills and attention to detail.",
    "Strong technical skills; could improve stakeholder communication.",
    "Exceeded all KPIs — strong candidate for promotion.",
]

# ---------------------------------------------------------------------------
# Build and insert data
# ---------------------------------------------------------------------------

def seed_ecommerce(cur):
    print("  Creating E-commerce tables...")
    cur.execute("""
        DROP TABLE IF EXISTS order_items, orders, addresses, products, categories, customers CASCADE;

        CREATE TABLE customers (
            id UUID PRIMARY KEY,
            email TEXT NOT NULL UNIQUE,
            full_name TEXT NOT NULL,
            phone TEXT,
            created_at TIMESTAMPTZ NOT NULL
        );
        CREATE TABLE categories (
            id UUID PRIMARY KEY,
            name TEXT NOT NULL,
            parent_id UUID REFERENCES categories(id)
        );
        CREATE TABLE products (
            id UUID PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT,
            price NUMERIC(10,2) NOT NULL,
            category_id UUID REFERENCES categories(id),
            stock_qty INTEGER NOT NULL DEFAULT 0,
            is_active BOOLEAN DEFAULT TRUE
        );
        CREATE TABLE addresses (
            id UUID PRIMARY KEY,
            customer_id UUID NOT NULL REFERENCES customers(id),
            street TEXT NOT NULL,
            city TEXT NOT NULL,
            state TEXT,
            country TEXT NOT NULL DEFAULT 'US',
            postal_code TEXT,
            is_default BOOLEAN DEFAULT FALSE
        );
        CREATE TABLE orders (
            id UUID PRIMARY KEY,
            customer_id UUID NOT NULL REFERENCES customers(id),
            status TEXT NOT NULL,
            total_amount NUMERIC(10,2) NOT NULL,
            shipping_address_id UUID REFERENCES addresses(id),
            created_at TIMESTAMPTZ NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL
        );
        CREATE TABLE order_items (
            id UUID PRIMARY KEY,
            order_id UUID NOT NULL REFERENCES orders(id),
            product_id UUID NOT NULL REFERENCES products(id),
            quantity INTEGER NOT NULL,
            unit_price NUMERIC(10,2) NOT NULL
        );
    """)

    # Categories
    cat_ids = {}
    for name, parent in CATEGORIES:
        cid = gid()
        cat_ids[name] = cid
        cur.execute("INSERT INTO categories VALUES (%s,%s,%s)", (cid, name, None))

    # Products
    prod_ids = []
    for cat_name, items in PRODUCTS_BY_CAT.items():
        for p_name, price, stock in items:
            pid = gid()
            prod_ids.append((pid, price))
            cur.execute(
                "INSERT INTO products VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (pid, p_name, None, price, cat_ids[cat_name], stock, True),
            )
    # Make 3 products out of stock
    for pid, _ in random.sample(prod_ids, 3):
        cur.execute("UPDATE products SET stock_qty=0 WHERE id=%s", (pid,))

    # Customers + addresses
    customer_ids = []
    addr_by_customer = {}
    for i in range(40):
        fn = random.choice(FIRST_NAMES)
        ln = random.choice(LAST_NAMES)
        cid = gid()
        email = f"{fn.lower()}.{ln.lower()}{random.randint(1,99)}@example.com"
        phone = f"+1-{random.randint(200,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}"
        created = rand_ts(365*3, 30)
        cur.execute("INSERT INTO customers VALUES (%s,%s,%s,%s,%s)",
                    (cid, email, f"{fn} {ln}", phone, created))
        customer_ids.append(cid)

        # Address
        aid = gid()
        city, state = random.choice(CITIES)
        street = f"{random.randint(100,9999)} {random.choice(STREETS)}"
        postal = str(random.randint(10000, 99999))
        cur.execute("INSERT INTO addresses VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                    (aid, cid, street, city, state, "US", postal, True))
        addr_by_customer[cid] = aid

    # Orders + order items
    for cid in customer_ids:
        num_orders = random.randint(1, 5)
        for _ in range(num_orders):
            oid = gid()
            status = random.choice(ORDER_STATUSES)
            created = rand_ts(365*2, 1)
            items = random.sample(prod_ids, random.randint(1, 4))
            total = 0.0
            order_rows = []
            for pid, price in items:
                qty = random.randint(1, 3)
                total += qty * float(price)
                order_rows.append((gid(), oid, pid, qty, price))

            cur.execute("INSERT INTO orders VALUES (%s,%s,%s,%s,%s,%s,%s)",
                        (oid, cid, status, round(total,2),
                         addr_by_customer[cid], created, created))
            cur.executemany("INSERT INTO order_items VALUES (%s,%s,%s,%s,%s)", order_rows)

    print(f"  E-commerce: 40 customers, {len(prod_ids)} products, orders inserted.")


def seed_hr(cur):
    print("  Creating HR tables...")
    cur.execute("""
        DROP TABLE IF EXISTS time_off, performance_reviews, salaries, employees, job_titles, departments CASCADE;

        CREATE TABLE departments (
            id UUID PRIMARY KEY,
            name TEXT NOT NULL,
            budget NUMERIC(15,2),
            location TEXT
        );
        CREATE TABLE job_titles (
            id UUID PRIMARY KEY,
            title TEXT NOT NULL,
            min_salary NUMERIC(12,2),
            max_salary NUMERIC(12,2)
        );
        CREATE TABLE employees (
            id UUID PRIMARY KEY,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            email TEXT NOT NULL UNIQUE,
            hire_date DATE NOT NULL,
            department_id UUID REFERENCES departments(id),
            job_title_id UUID REFERENCES job_titles(id),
            manager_id UUID REFERENCES employees(id),
            is_active BOOLEAN DEFAULT TRUE
        );
        CREATE TABLE salaries (
            id UUID PRIMARY KEY,
            employee_id UUID NOT NULL REFERENCES employees(id),
            amount NUMERIC(12,2) NOT NULL,
            effective_date DATE NOT NULL,
            end_date DATE
        );
        CREATE TABLE performance_reviews (
            id UUID PRIMARY KEY,
            employee_id UUID NOT NULL REFERENCES employees(id),
            reviewer_id UUID NOT NULL REFERENCES employees(id),
            rating INTEGER NOT NULL,
            comments TEXT,
            review_date DATE NOT NULL
        );
        CREATE TABLE time_off (
            id UUID PRIMARY KEY,
            employee_id UUID NOT NULL REFERENCES employees(id),
            type TEXT NOT NULL,
            start_date DATE NOT NULL,
            end_date DATE NOT NULL,
            status TEXT NOT NULL
        );
    """)

    # Departments
    dept_ids = {}
    for name, budget, location in DEPARTMENTS:
        did = gid()
        dept_ids[name] = did
        cur.execute("INSERT INTO departments VALUES (%s,%s,%s,%s)", (did, name, budget, location))

    # Job titles
    jt_ids = {}
    for title, mn, mx in JOB_TITLES:
        jid = gid()
        jt_ids[title] = jid
        cur.execute("INSERT INTO job_titles VALUES (%s,%s,%s,%s)", (jid, title, mn, mx))

    # Dept → job title mapping
    dept_jt = {
        "Engineering": ["Junior Software Engineer","Senior Software Engineer","Engineering Manager"],
        "Sales":       ["Sales Representative","Sales Manager"],
        "Marketing":   ["Marketing Specialist"],
        "HR":          ["HR Generalist"],
        "Finance":     ["Financial Analyst"],
    }
    dept_sizes = {"Engineering":10, "Sales":6, "Marketing":4, "HR":3, "Finance":4}

    # Employees (no manager_id yet — add after)
    emp_ids = []  # list of (id, dept, jt_title, hire_date)
    for dept_name, count in dept_sizes.items():
        for _ in range(count):
            eid = gid()
            fn = random.choice(EMP_FIRST)
            ln = random.choice(EMP_LAST)
            email = f"{fn.lower()}.{ln.lower()}{random.randint(1,99)}@company.com"
            jt = random.choice(dept_jt[dept_name])
            hire = rand_date(365*6, 60)
            cur.execute(
                "INSERT INTO employees(id,first_name,last_name,email,hire_date,department_id,job_title_id,is_active)"
                " VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                (eid, fn, ln, email, hire, dept_ids[dept_name], jt_ids[jt], True),
            )
            emp_ids.append((eid, dept_name, jt, hire))

    # Assign managers (one Engineering Manager manages Engineering engineers)
    eng_manager = next((e for e in emp_ids if e[2]=="Engineering Manager"), None)
    sales_manager = next((e for e in emp_ids if e[2]=="Sales Manager"), None)
    if eng_manager:
        for eid, dept, jt, _ in emp_ids:
            if dept=="Engineering" and jt!="Engineering Manager":
                cur.execute("UPDATE employees SET manager_id=%s WHERE id=%s", (eng_manager[0], eid))
    if sales_manager:
        for eid, dept, jt, _ in emp_ids:
            if dept=="Sales" and jt=="Sales Representative":
                cur.execute("UPDATE employees SET manager_id=%s WHERE id=%s", (sales_manager[0], eid))

    # Mark 2 employees inactive
    for eid, *_ in random.sample(emp_ids, 2):
        cur.execute("UPDATE employees SET is_active=FALSE WHERE id=%s", (eid,))

    # Salaries
    jt_salary_range = {t: (mn, mx) for t, mn, mx in JOB_TITLES}
    for eid, dept, jt, hire_date in emp_ids:
        mn, mx = jt_salary_range.get(jt, (60000, 100000))
        # Initial salary (at hire)
        initial = round(random.uniform(float(mn), float(mx) * 0.85), -2)
        # Make 4 employees above band to make the complex query interesting
        if dept == "Engineering" and random.random() < 0.25:
            initial = round(float(mx) * random.uniform(1.05, 1.2), -2)

        yrs = (date.today() - hire_date).days // 365
        if yrs >= 2:
            # Has salary history
            old_end = hire_date + timedelta(days=random.randint(365, 365*2))
            cur.execute("INSERT INTO salaries VALUES (%s,%s,%s,%s,%s)",
                        (gid(), eid, initial, hire_date, old_end))
            new_sal = round(initial * random.uniform(1.05, 1.15), -2)
            cur.execute("INSERT INTO salaries VALUES (%s,%s,%s,%s,%s)",
                        (gid(), eid, new_sal, old_end + timedelta(days=1), None))
        else:
            cur.execute("INSERT INTO salaries VALUES (%s,%s,%s,%s,%s)",
                        (gid(), eid, initial, hire_date, None))

    # Performance reviews (2-3 years of annual reviews)
    manager_ids = ([eng_manager[0]] if eng_manager else []) + ([sales_manager[0]] if sales_manager else [])
    if not manager_ids:
        manager_ids = [emp_ids[0][0]]

    for eid, dept, jt, hire_date in emp_ids:
        yrs = min(3, (date.today() - hire_date).days // 365)
        for y in range(1, yrs + 1):
            review_date = date(date.today().year - (yrs - y), random.randint(3, 5), random.randint(1, 28))
            rating = random.choices([1,2,3,4,5], weights=[3,8,30,35,24])[0]
            reviewer = random.choice(manager_ids)
            if reviewer == eid:
                reviewer = emp_ids[0][0]
            cur.execute("INSERT INTO performance_reviews VALUES (%s,%s,%s,%s,%s,%s)",
                        (gid(), eid, reviewer, rating,
                         random.choice(REVIEW_COMMENTS), review_date))

    # Time off
    types = ["vacation","sick","parental","unpaid"]
    statuses = ["approved","approved","approved","pending","rejected"]
    for eid, *_ in random.sample(emp_ids, 15):
        start = rand_date(180, 10)
        end = start + timedelta(days=random.randint(1, 14))
        cur.execute("INSERT INTO time_off VALUES (%s,%s,%s,%s,%s,%s)",
                    (gid(), eid, random.choice(types), start, end, random.choice(statuses)))

    print(f"  HR: {len(emp_ids)} employees, salaries, reviews, time-off inserted.")


def main():
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    try:
        with conn.cursor() as cur:
            seed_ecommerce(cur)
            seed_hr(cur)
        conn.commit()
        print("Sample data seeded successfully.")
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


if __name__ == "__main__":
    main()
