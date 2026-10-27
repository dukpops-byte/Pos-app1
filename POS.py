
import os
import io
import csv
import re
import psycopg2
from psycopg2.extras import RealDictCursor
import secrets
import hmac
import math
from datetime import datetime
from urllib.parse import quote
from functools import wraps

from flask import (
    Flask, request, redirect, url_for, session,
    flash, render_template_string, make_response
)
from werkzeug.security import generate_password_hash, check_password_hash

try:
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False

app = Flask(__name__)

# Configure these environment variables in Render (Environment):
# POS_SECRET_KEY, POS_SUPERADMIN_USERNAME, POS_SUPERADMIN_PASSWORD.
# The random fallback is for local development only; set POS_SECRET_KEY in production
# so sessions remain valid across restarts/deploys.
app.secret_key = os.environ.get("POS_SECRET_KEY") or secrets.token_hex(32)
# Harden browser session cookies. Render deployments should use HTTPS.
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SECURE=bool(os.environ.get("RENDER")) or os.environ.get("POS_COOKIE_SECURE", "").lower() in ("1", "true", "yes"),
    SESSION_COOKIE_SAMESITE="Lax",
    PERMANENT_SESSION_LIFETIME=8 * 60 * 60,
)
SUPERADMIN_USERNAME = os.environ.get("POS_SUPERADMIN_USERNAME", "").strip()
SUPERADMIN_PASSWORD = os.environ.get("POS_SUPERADMIN_PASSWORD", "")
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL must be set to a PostgreSQL connection string.")

BUSINESS_TYPES = [
    "Food & Restaurant", "Clothing & Footwear", "Grocery & Supermarket",
    "Electronics & Mobile Accessories", "Cosmetics & Personal Care",
    "Pharmacy", "Hardware & Building Materials", "Furniture & Home Decor",
    "Books & Stationery", "Automotive & Spare Parts", "Jewellery",
    "Pet Supplies", "Sports & Fitness", "Toys & Baby Products",
    "Electrical Supplies", "Agriculture & Garden", "Other Retail"
]

CATEGORY_MAP = {
    "Food & Restaurant": ["Bakery & Cakes", "Beverages", "Fast Food", "Snacks", "Dairy Products", "Sweets & Desserts", "Fruits & Vegetables", "Meat & Seafood", "Groceries & Ingredients", "Packaged Food", "Other Food Products"],
    "Clothing & Footwear": ["Men's Clothing", "Women's Clothing", "Kids' Clothing", "Footwear", "Bags", "Fashion Accessories"],
    "Grocery & Supermarket": ["Grains & Pulses", "Fruits & Vegetables", "Dairy Products", "Beverages", "Packaged Food", "Household Supplies", "Personal Care"],
    "Electronics & Mobile Accessories": ["Mobile Phones", "Chargers & Cables", "Audio", "Computer Accessories", "Smart Devices", "Other Electronics"],
    "Cosmetics & Personal Care": ["Skincare", "Haircare", "Makeup", "Fragrances", "Personal Hygiene", "Other Personal Care"],
    "Pharmacy": ["Prescription Products", "OTC Products", "First Aid", "Medical Supplies", "Other Pharmacy Products"],
    "Hardware & Building Materials": ["Tools", "Fasteners", "Paint", "Plumbing", "Building Materials", "Other Hardware"],
    "Furniture & Home Decor": ["Furniture", "Lighting", "Home Decor", "Kitchenware", "Bedding", "Other Home Products"],
    "Books & Stationery": ["Books", "Notebooks", "Pens & Pencils", "Office Supplies", "Art Supplies", "Other Stationery"],
    "Automotive & Spare Parts": ["Engine Parts", "Accessories", "Lubricants", "Electrical Parts", "Tools", "Other Automotive Products"],
    "Jewellery": ["Gold Jewellery", "Silver Jewellery", "Imitation Jewellery", "Watches", "Accessories", "Other Jewellery"],
    "Pet Supplies": ["Pet Food", "Toys", "Grooming", "Accessories", "Healthcare Products", "Other Pet Products"],
    "Sports & Fitness": ["Fitness Equipment", "Sportswear", "Outdoor Sports", "Accessories", "Other Sports Products"],
    "Toys & Baby Products": ["Toys", "Baby Clothing", "Baby Care", "Feeding Products", "Other Baby Products"],
    "Electrical Supplies": ["Wires & Cables", "Switches", "Lighting", "Electrical Tools", "Other Electrical Products"],
    "Agriculture & Garden": ["Seeds", "Garden Tools", "Plant Care", "Irrigation", "Other Agriculture Products"],
    "Other Retail": ["General Products", "Other Products"]
}


def now_text():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class PGCursor:
    def __init__(self, cursor, lastrowid=None):
        self._cursor = cursor
        self.lastrowid = lastrowid

    def fetchone(self):
        return self._cursor.fetchone()

    def fetchall(self):
        return self._cursor.fetchall()

    @property
    def rowcount(self):
        return self._cursor.rowcount


class PGConnection:
    """Small compatibility layer for the app's existing SQLite-style SQL."""
    def __init__(self):
        self._conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor, connect_timeout=15)
        self._last_rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if exc_type:
                self._conn.rollback()
            else:
                self._conn.commit()
        finally:
            self._conn.close()
        return False

    def execute(self, sql, params=()):
        sql = sql.strip()
        if sql.upper() == "BEGIN IMMEDIATE":
            sql = "BEGIN"
        if sql.upper().startswith("PRAGMA TABLE_INFO("):
            table = re.search(r"PRAGMA\s+table_info\(([^)]+)\)", sql, re.I).group(1).strip()
            cur = self._conn.cursor()
            cur.execute("SELECT column_name AS name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name=%s ORDER BY ordinal_position", (table,))
            return PGCursor(cur)
        if re.match(r"SELECT\s+changes\(\)", sql, re.I):
            cur = self._conn.cursor()
            cur.execute("SELECT %s AS changes", (self._last_rowcount,))
            return PGCursor(cur)
        sql = re.sub(r"INTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT", "BIGSERIAL PRIMARY KEY", sql, flags=re.I)
        sql = re.sub(r"INSERT\s+OR\s+IGNORE\s+INTO", "INSERT INTO", sql, flags=re.I)
        if re.match(r"INSERT\s+INTO\s+(categories)\b", sql, re.I) and "ON CONFLICT" not in sql.upper():
            sql = sql.rstrip().rstrip(';') + " ON CONFLICT (business_id, name) DO NOTHING"
        sql = sql.replace("?", "%s")
        cur = self._conn.cursor()
        wants_id = bool(re.match(r"INSERT\s+INTO\s+(businesses|sales)\b", sql, re.I)) and "RETURNING" not in sql.upper()
        if wants_id:
            sql = sql.rstrip().rstrip(';') + " RETURNING id"
        cur.execute(sql, params or ())
        self._last_rowcount = cur.rowcount
        lastrowid = None
        if wants_id:
            row = cur.fetchone()
            lastrowid = row["id"] if row else None
        return PGCursor(cur, lastrowid=lastrowid)

    def executescript(self, script):
        for statement in script.split(';'):
            if statement.strip():
                self.execute(statement)


def connect():
    return PGConnection()


def add_columns(c, table, definitions):
    existing = {r["name"] for r in c.execute(
        "SELECT column_name AS name FROM information_schema.columns "
        "WHERE table_schema=current_schema() AND table_name=?", (table,)
    ).fetchall()}
    for name, definition in definitions:
        if name not in existing:
            # Existing definitions are simple PostgreSQL-compatible TEXT/REAL/INTEGER types.
            c.execute(f"ALTER TABLE {table} ADD COLUMN {name} {definition}")


def init_db():
    with connect() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS businesses(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            category TEXT NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS users(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            role TEXT NOT NULL,
            business_id INTEGER
        );

        CREATE TABLE IF NOT EXISTS categories(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            business_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            UNIQUE(business_id,name),
            FOREIGN KEY(business_id) REFERENCES businesses(id)
        );

        CREATE TABLE IF NOT EXISTS products(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            business_id INTEGER NOT NULL,
            category_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            price REAL NOT NULL DEFAULT 0,
            stock INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY(business_id) REFERENCES businesses(id),
            FOREIGN KEY(category_id) REFERENCES categories(id)
        );

        CREATE TABLE IF NOT EXISTS customers(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            business_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            phone TEXT NOT NULL,
            username TEXT UNIQUE,
            password_hash TEXT,
            points INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            UNIQUE(business_id,phone),
            FOREIGN KEY(business_id) REFERENCES businesses(id)
        );

        CREATE TABLE IF NOT EXISTS sales(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            business_id INTEGER NOT NULL,
            customer_id INTEGER,
            customer_name TEXT NOT NULL,
            customer_phone TEXT,
            total REAL NOT NULL,
            subtotal REAL NOT NULL DEFAULT 0,
            points_earned INTEGER NOT NULL DEFAULT 0,
            points_redeemed INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            FOREIGN KEY(business_id) REFERENCES businesses(id),
            FOREIGN KEY(customer_id) REFERENCES customers(id)
        );

        CREATE TABLE IF NOT EXISTS sale_items(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sale_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            product_name TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            unit_price REAL NOT NULL,
            line_total REAL NOT NULL,
            FOREIGN KEY(sale_id) REFERENCES sales(id),
            FOREIGN KEY(product_id) REFERENCES products(id)
        );

        CREATE TABLE IF NOT EXISTS stock_adjustments(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            business_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            user_id INTEGER,
            adjustment_type TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            old_stock INTEGER NOT NULL,
            new_stock INTEGER NOT NULL,
            reason TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL
        );
        """)

        add_columns(c, "users", [("active", "INTEGER NOT NULL DEFAULT 1")])
        add_columns(c, "products", [("active", "INTEGER NOT NULL DEFAULT 1")])
        add_columns(c, "businesses", [
            ("gstin", "TEXT NOT NULL DEFAULT ''"),
            ("address", "TEXT NOT NULL DEFAULT ''"),
            ("city", "TEXT NOT NULL DEFAULT ''"),
            ("state", "TEXT NOT NULL DEFAULT ''"),
            ("pincode", "TEXT NOT NULL DEFAULT ''"),
            ("gst_rate", "REAL NOT NULL DEFAULT 0")
        ])
        add_columns(c, "sales", [
            ("customer_phone", "TEXT"),
            ("subtotal", "REAL NOT NULL DEFAULT 0"),
            ("points_earned", "INTEGER NOT NULL DEFAULT 0"),
            ("points_redeemed", "INTEGER NOT NULL DEFAULT 0"),
            ("gst_rate", "REAL NOT NULL DEFAULT 0"),
            ("gst_amount", "REAL NOT NULL DEFAULT 0")
        ])
        add_columns(c, "customers", [
            ("username", "TEXT"),
            ("password_hash", "TEXT"),
            ("created_at", "TEXT")
        ])

        c.execute(
            "UPDATE customers SET created_at=? WHERE created_at IS NULL",
            (now_text(),)
        )

        # Existing business admins remain business owners (role=admin).
        # The configured environment credentials are the source of truth for the
        # single superadmin account. Never embed a default password in source code.
        if SUPERADMIN_USERNAME and SUPERADMIN_PASSWORD:
            if len(SUPERADMIN_PASSWORD) < 12:
                raise RuntimeError("POS_SUPERADMIN_PASSWORD must be at least 12 characters.")
            conflict = c.execute(
                "SELECT id, role FROM users WHERE username=? AND role!='superadmin'",
                (SUPERADMIN_USERNAME,)
            ).fetchone()
            if conflict:
                raise RuntimeError(
                    "POS_SUPERADMIN_USERNAME is already used by a non-superadmin account. "
                    "Set a unique username in the environment."
                )
            existing_superadmin = c.execute(
                "SELECT id FROM users WHERE role='superadmin' ORDER BY id LIMIT 1"
            ).fetchone()
            if existing_superadmin:
                c.execute(
                    "UPDATE users SET username=?, password=?, business_id=NULL WHERE id=?",
                    (SUPERADMIN_USERNAME, generate_password_hash(SUPERADMIN_PASSWORD),
                     existing_superadmin["id"])
                )
                # Remove any duplicate legacy superadmin accounts.
                c.execute(
                    "DELETE FROM users WHERE role='superadmin' AND id!=?",
                    (existing_superadmin["id"],)
                )
            else:
                c.execute(
                    "INSERT INTO users(username,password,role,business_id) VALUES(?,?,?,NULL)",
                    (SUPERADMIN_USERNAME, generate_password_hash(SUPERADMIN_PASSWORD), "superadmin")
                )
        else:
            app.logger.warning(
                "POS_SUPERADMIN_USERNAME/POS_SUPERADMIN_PASSWORD are not both set. "
                "No superadmin account was bootstrapped from environment."
            )


def current_role():
    return session.get("role")


def login_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not session.get("user_id"):
            return redirect(url_for("login"))
        return fn(*args, **kwargs)
    return wrapper


def roles_required(*roles):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            if not session.get("user_id"):
                return redirect(url_for("login"))
            if current_role() not in roles:
                flash("You do not have permission to access that page.")
                return redirect(url_for("home"))
            return fn(*args, **kwargs)
        return wrapper
    return decorator


owner_required = roles_required("admin")
billing_access_required = roles_required("admin", "billing_user")
platform_required = roles_required("platform_admin")
superadmin_required = roles_required("superadmin")


def get_business():
    if current_role() not in ("admin", "billing_user"):
        return None
    with connect() as c:
        return c.execute(
            "SELECT * FROM businesses WHERE id=?",
            (session.get("business_id"),)
        ).fetchone()


BASE = """
<!doctype html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{{ title }}</title>
<style>
*{box-sizing:border-box}
body{font-family:Arial,sans-serif;background:#f1f5f9;color:#172033;margin:0}
header{background:#14532d;color:white;padding:15px}
header a{color:white;text-decoration:none;margin:5px 10px 5px 0;display:inline-block}
main{max-width:1100px;margin:18px auto;padding:0 12px}
.card{background:white;padding:17px;border-radius:12px;margin-bottom:15px;box-shadow:0 2px 8px #0001}
input,select,button,textarea{padding:11px;margin:5px 0;border:1px solid #cbd5e1;border-radius:6px;max-width:100%}
input,select,textarea{width:100%}
button,.btn{background:#15803d;color:white;border:0;padding:11px 15px;border-radius:6px;cursor:pointer;text-decoration:none;display:inline-block}
.red{background:#b91c1c}.whatsapp{background:#128c45}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(210px,1fr));gap:12px}
.tablewrap{overflow-x:auto}
table{width:100%;border-collapse:collapse;background:white}
th,td{padding:10px;text-align:left;border-bottom:1px solid #e2e8f0}
.notice{background:#fef3c7;padding:11px;border-radius:6px;margin:8px 0}
small{color:#64748b}
</style>
</head>
<body>
<header>
<strong>POS & Customer Loyalty</strong><br>
{% if session.get('user_id') %}
<a href="{{ url_for('home') }}">Dashboard</a>
{% if session.get('role') == 'superadmin' %}
<a href="{{ url_for('platform_admins') }}">Platform Admins</a>
{% elif session.get('role') == 'platform_admin' %}
<a href="{{ url_for('businesses') }}">Businesses</a>
{% elif session.get('role') == 'admin' %}
<a href="{{ url_for('products') }}">Products</a>
<a href="{{ url_for('billing') }}">Billing</a>
<a href="{{ url_for('customers') }}">Customers</a>
<a href="{{ url_for('reports') }}">Reports</a>
<a href="{{ url_for('loyalty_accounts') }}">Loyalty Accounts</a>
<a href="{{ url_for('billing_users') }}">Billing Users</a>
<a href="{{ url_for('business_settings') }}">Business Settings</a>
{% elif session.get('role') == 'billing_user' %}
<a href="{{ url_for('billing') }}">Billing</a>
{% elif session.get('role') == 'customer' %}
<a href="{{ url_for('customer_home') }}">My Account</a>
{% endif %}
<a href="{{ url_for('logout') }}">Logout ({{ session.get('username') }})</a>
{% else %}
<a href="{{ url_for('login') }}">Login</a>
{% endif %}
</header>
<main>
{% with messages=get_flashed_messages() %}
{% for m in messages %}<div class="notice">{{ m }}</div>{% endfor %}
{% endwith %}
{{ content|safe }}
</main>
</body>
</html>
"""


def page(title, template, **kwargs):
    content = render_template_string(template, **kwargs)
    # Add CSRF tokens to every server-rendered form, including multipart forms.
    token = session.get("_csrf_token")
    if not token:
        token = secrets.token_urlsafe(32)
        session["_csrf_token"] = token
    hidden = '<input type="hidden" name="_csrf_token" value="' + token + '">'
    content = re.sub(r"(<form\\b[^>]*>)", lambda match: match.group(1) + hidden,
                     content, flags=re.IGNORECASE)
    response = make_response(render_template_string(BASE, title=title, content=content))
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Referrer-Policy"] = "same-origin"
    response.headers["X-Frame-Options"] = "SAMEORIGIN"
    return response


@app.before_request
def enforce_session_and_csrf():
    # Revalidate authorization from PostgreSQL on every authenticated request.
    role = session.get("role")
    user_id = session.get("user_id")
    if user_id and role in ("superadmin", "platform_admin", "admin", "billing_user"):
        try:
            with connect() as c:
                user = c.execute(
                    "SELECT id, username, role, business_id, active FROM users WHERE id=?",
                    (user_id,)
                ).fetchone()
            if (not user or not int(user["active"] or 0)
                    or user["role"] != role
                    or user["username"] != session.get("username")
                    or user["business_id"] != session.get("business_id")):
                session.clear()
                if request.endpoint != "login":
                    return redirect(url_for("login"))
        except psycopg2.Error:
            # Fail closed rather than trusting a stale session during DB errors.
            session.clear()
            app.logger.exception("Session authorization check failed")
            return "Authentication service temporarily unavailable.", 503
    elif user_id and role == "customer":
        try:
            with connect() as c:
                customer = c.execute(
                    "SELECT id, username, phone, business_id FROM customers WHERE id=? AND business_id=?",
                    (session.get("customer_id", user_id), session.get("business_id"))
                ).fetchone()
            if not customer:
                session.clear()
                if request.endpoint != "login":
                    return redirect(url_for("login"))
        except psycopg2.Error:
            session.clear()
            app.logger.exception("Customer session check failed")
            return "Authentication service temporarily unavailable.", 503

    if request.method == "POST":
        # Login is protected too; the login form receives a token on GET.
        expected = session.get("_csrf_token", "")
        supplied = request.form.get("_csrf_token", "")
        if not expected or not supplied or not hmac.compare_digest(expected, supplied):
            return "Invalid or missing CSRF token. Reload the page and try again.", 400


@app.route("/")
def index():
    if not session.get("user_id"):
        return redirect(url_for("login"))
    if current_role() == "customer":
        return redirect(url_for("customer_home"))
    return redirect(url_for("home"))


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        with connect() as c:
            user = c.execute(
                "SELECT * FROM users WHERE username=?", (username,)
            ).fetchone()

            if user and int(user["active"] if "active" in user.keys() else 1) == 1 and check_password_hash(user["password"], password):
                session.clear()
                session.permanent = True
                session["user_id"] = user["id"]
                session["username"] = user["username"]
                session["role"] = user["role"]
                session["business_id"] = user["business_id"]
                return redirect(url_for("home"))

            customers = c.execute(
                "SELECT * FROM customers WHERE username=? OR phone=? ORDER BY id DESC",
                (username, username)
            ).fetchall()

            for cust in customers:
                if cust["password_hash"] and check_password_hash(cust["password_hash"], password):
                    session.clear()
                    session.permanent = True
                    session["user_id"] = cust["id"]
                    session["username"] = cust["username"] or cust["phone"]
                    session["role"] = "customer"
                    session["business_id"] = cust["business_id"]
                    session["customer_id"] = cust["id"]
                    return redirect(url_for("customer_home"))

        flash("Invalid username or password.")

    return page("Login", """
    <div class="card" style="max-width:450px;margin:35px auto">
    <h2>Login</h2>
    <form method="post">
    <label>Username or customer phone</label>
    <input name="username" required autocomplete="username">
    <label>Password</label>
    <input type="password" name="password" required autocomplete="current-password">
    <button>Login</button>
    </form>
    </div>
    """)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/home")
@login_required
def home():
    role = current_role()
    if role == "customer":
        return redirect(url_for("customer_home"))
    if role == "superadmin":
        with connect() as c:
            count = c.execute("SELECT COUNT(*) n FROM users WHERE role='platform_admin'").fetchone()["n"]
        return page("Superadmin", """
        <div class="card"><h2>Superadmin Dashboard</h2>
        <p>Platform admins: {{ count }}</p>
        <a class="btn" href="{{ url_for('platform_admins') }}">Manage Platform Admins</a>
        <a class="btn" href="{{ url_for('businesses') }}">Manage Business Logins</a></div>
        """, count=count)

    if role == "platform_admin":
        with connect() as c:
            businesses_count = c.execute("SELECT COUNT(*) n FROM businesses").fetchone()["n"]
        return page("Platform Admin", """
        <div class="card"><h2>Platform Admin Dashboard</h2>
        <p>Businesses: {{ businesses_count }}</p>
        <a class="btn" href="{{ url_for('businesses') }}">Manage Businesses</a></div>
        """, businesses_count=businesses_count)

    bid = session.get("business_id")
    if not bid:
        session.clear()
        return redirect(url_for("login"))

    with connect() as c:
        products_count = c.execute("SELECT COUNT(*) n FROM products WHERE business_id=?", (bid,)).fetchone()["n"]
        sales = c.execute("SELECT COUNT(*) n,COALESCE(SUM(total),0) total FROM sales WHERE business_id=?", (bid,)).fetchone()
        customers_count = c.execute("SELECT COUNT(*) n FROM customers WHERE business_id=?", (bid,)).fetchone()["n"]

    if role == "billing_user":
        return page("Billing Dashboard", """
        <div class="card"><h2>Billing User Dashboard</h2>
        <p>You can generate bills and redeem customer loyalty points.</p>
        <a class="btn" href="{{ url_for('billing') }}">Open Billing</a></div>
        """)

    return page("Business Dashboard", """
    <h2>Business Owner Dashboard</h2><div class="grid">
    <div class="card"><h3>Products</h3><h2>{{ products_count }}</h2></div>
    <div class="card"><h3>Bills</h3><h2>{{ sales.n }}</h2></div>
    <div class="card"><h3>Total Sales</h3><h2>₹{{ "%.2f"|format(sales.total) }}</h2></div>
    <div class="card"><h3>Customers</h3><h2>{{ customers_count }}</h2></div>
    </div>""", products_count=products_count, sales=sales, customers_count=customers_count)


@app.route("/platform-admins", methods=["GET", "POST"])
@superadmin_required
def platform_admins():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        if not username or len(password) < 8:
            flash("Enter a username and password with at least 8 characters.")
            return redirect(url_for("platform_admins"))
        try:
            with connect() as c:
                c.execute("INSERT INTO users(username,password,role,business_id,active) VALUES(?,?,?,NULL,1)",
                          (username, generate_password_hash(password), "platform_admin"))
            flash("Platform admin created.")
        except psycopg2.IntegrityError:
            flash("Username already exists.")
        return redirect(url_for("platform_admins"))
    with connect() as c:
        users = c.execute("SELECT id,username,active FROM users WHERE role='platform_admin' ORDER BY username").fetchall()
    return page("Platform Admins", """
    <div class="card"><h2>Create Platform Admin</h2><form method="post">
    <label>Username</label><input name="username" required>
    <label>Password (minimum 8 characters)</label><input type="password" name="password" minlength="8" required>
    <button>Create Platform Admin</button></form></div>
    <div class="card"><h2>Manage Platform Admins</h2><div class="tablewrap"><table>
    <tr><th>Username</th><th>Status</th><th>Actions</th></tr>
    {% for u in users %}<tr><td>{{ u.username }}</td><td>{{ 'Active' if u.active else 'Inactive' }}</td><td>
    <form method="post" action="{{ url_for('manage_user', user_id=u.id) }}">
    <input type="password" name="password" minlength="8" placeholder="New password (optional)">
    <button name="action" value="password">Change password</button>
    <button name="action" value="{{ 'deactivate' if u.active else 'activate' }}">{{ 'Deactivate' if u.active else 'Activate' }}</button>
    <button name="action" value="delete" onclick="return confirm('Delete this platform admin?')">Delete</button>
    </form></td></tr>{% else %}<tr><td colspan="3">No platform admins created.</td></tr>{% endfor %}
    </table></div></div>""", users=users)


@app.route("/manage-user/<int:user_id>", methods=["POST"])
@superadmin_required
def manage_user(user_id):
    action = request.form.get("action", "")
    with connect() as c:
        user = c.execute("SELECT id,username,role FROM users WHERE id=?", (user_id,)).fetchone()
        if not user or user["role"] not in ("platform_admin", "admin"):
            flash("User not found or cannot be managed here.")
        elif user["id"] == session.get("user_id"):
            flash("You cannot change or delete your own account here.")
        elif action == "password":
            password = request.form.get("password", "")
            if len(password) < 8:
                flash("New password must contain at least 8 characters.")
            else:
                c.execute("UPDATE users SET password=? WHERE id=?", (generate_password_hash(password), user_id))
                flash("Password changed.")
        elif action in ("activate", "deactivate"):
            c.execute("UPDATE users SET active=? WHERE id=?", (1 if action == "activate" else 0, user_id))
            flash("User status updated.")
        elif action == "delete":
            c.execute("DELETE FROM users WHERE id=?", (user_id,))
            flash("Login account deleted. Business records were kept.")
        else:
            flash("Invalid action.")
    return redirect(url_for("platform_admins") if user and user["role"] == "platform_admin" else url_for("businesses"))


@app.route("/businesses", methods=["GET", "POST"])
@roles_required("platform_admin", "superadmin")
def businesses():
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        category = request.form.get("category", "")
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        if not name or category not in BUSINESS_TYPES or not username or len(password) < 8:
            flash("Enter valid details. Owner password must have at least 8 characters.")
            return redirect(url_for("businesses"))
        try:
            with connect() as c:
                cur = c.execute("INSERT INTO businesses(name,category,created_at) VALUES(?,?,?)", (name, category, now_text()))
                bid = cur.lastrowid
                for cat in CATEGORY_MAP.get(category, ["Other Products"]):
                    c.execute("INSERT INTO categories(business_id,name) VALUES(?,?) ON CONFLICT (business_id,name) DO NOTHING", (bid, cat))
                c.execute("INSERT INTO users(username,password,role,business_id,active) VALUES(?,?,?,?,1)",
                          (username, generate_password_hash(password), "admin", bid))
            flash("Business and business-owner account created.")
        except psycopg2.IntegrityError:
            flash("That username already exists.")
        return redirect(url_for("businesses"))
    with connect() as c:
        rows = c.execute("""SELECT b.*,u.id owner_id,u.username owner_username,u.active owner_active
            FROM businesses b LEFT JOIN users u ON u.business_id=b.id AND u.role='admin' ORDER BY b.id DESC""").fetchall()
    return page("Businesses", """
    <div class="card"><h2>Create Business</h2><form method="post"><div class="grid">
    <div><label>Business name</label><input name="name" required></div><div><label>Business type</label><select name="category">{% for x in business_types %}<option>{{ x }}</option>{% endfor %}</select></div>
    <div><label>Business owner username</label><input name="username" required></div><div><label>Business owner password (min 8)</label><input type="password" name="password" minlength="8" required></div>
    </div><button>Create Business</button></form></div>
    <div class="card"><h2>Businesses and Business Logins</h2><div class="tablewrap"><table><tr><th>Name</th><th>Type</th><th>Owner login</th><th>Status</th><th>Actions</th></tr>
    {% for b in rows %}<tr><td>{{ b.name }}</td><td>{{ b.category }}</td><td>{{ b.owner_username or '-' }}</td><td>{{ ('Active' if b.owner_active else 'Inactive') if b.owner_id else 'No login' }}</td><td>
    {% if b.owner_id %}<form method="post" action="{{ url_for('manage_user', user_id=b.owner_id) }}"><input type="password" name="password" minlength="8" placeholder="New password (optional)">
    <button name="action" value="password">Change password</button><button name="action" value="{{ 'deactivate' if b.owner_active else 'activate' }}">{{ 'Deactivate' if b.owner_active else 'Activate' }}</button>
    <button name="action" value="delete" onclick="return confirm('Delete this business login? Business records will remain.')">Delete login</button></form>{% endif %}</td></tr>
    {% else %}<tr><td colspan="5">No businesses yet.</td></tr>{% endfor %}</table></div></div>""", rows=rows, business_types=BUSINESS_TYPES)


@app.route("/billing-users", methods=["GET", "POST"])
@owner_required
def billing_users():
    bid = session["business_id"]
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        if not username or len(password) < 8:
            flash("Enter a username and password of at least 8 characters.")
            return redirect(url_for("billing_users"))
        try:
            with connect() as c:
                c.execute("INSERT INTO users(username,password,role,business_id,active) VALUES(?,?,?,?,1)",
                          (username, generate_password_hash(password), "billing_user", bid))
            flash("Billing user created.")
        except psycopg2.IntegrityError:
            flash("Username already exists.")
        return redirect(url_for("billing_users"))
    with connect() as c:
        users = c.execute("SELECT id,username,active FROM users WHERE business_id=? AND role='billing_user' ORDER BY username", (bid,)).fetchall()
    return page("Billing Users", """
    <div class="card"><h2>Create Billing User</h2><p>Billing users can generate bills, redeem points, and view/print individual bills only.</p>
    <form method="post"><label>Username</label><input name="username" required><label>Password (minimum 8 characters)</label><input type="password" name="password" minlength="8" required><button>Create Billing User</button></form></div>
    <div class="card"><h2>Manage Billing Users</h2><div class="tablewrap"><table><tr><th>Username</th><th>Status</th><th>Actions</th></tr>
    {% for u in users %}<tr><td>{{ u.username }}</td><td>{{ 'Active' if u.active else 'Inactive' }}</td><td><form method="post" action="{{ url_for('manage_billing_user', user_id=u.id) }}">
    <input type="password" name="password" minlength="8" placeholder="New password (optional)"><button name="action" value="password">Change password</button>
    <button name="action" value="{{ 'deactivate' if u.active else 'activate' }}">{{ 'Deactivate' if u.active else 'Activate' }}</button>
    <button name="action" value="delete" onclick="return confirm('Delete this billing user?')">Delete</button></form></td></tr>
    {% else %}<tr><td colspan="3">No billing users yet.</td></tr>{% endfor %}</table></div></div>""", users=users)


@app.route("/manage-billing-user/<int:user_id>", methods=["POST"])
@owner_required
def manage_billing_user(user_id):
    action = request.form.get("action", "")
    with connect() as c:
        user = c.execute("SELECT id FROM users WHERE id=? AND business_id=? AND role='billing_user'", (user_id, session.get("business_id"))).fetchone()
        if not user:
            flash("Billing user not found.")
        elif action == "password":
            password = request.form.get("password", "")
            if len(password) < 8: flash("New password must contain at least 8 characters.")
            else:
                c.execute("UPDATE users SET password=? WHERE id=?", (generate_password_hash(password), user_id)); flash("Billing user password changed.")
        elif action in ("activate", "deactivate"):
            c.execute("UPDATE users SET active=? WHERE id=?", (1 if action == "activate" else 0, user_id)); flash("Billing user status updated.")
        elif action == "delete":
            c.execute("DELETE FROM users WHERE id=?", (user_id,)); flash("Billing user deleted.")
        else: flash("Invalid action.")
    return redirect(url_for("billing_users"))


@app.route("/business-settings", methods=["GET", "POST"])
@owner_required
def business_settings():
    bid = session["business_id"]
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        gstin = request.form.get("gstin", "").strip().upper()
        address = request.form.get("address", "").strip()
        city = request.form.get("city", "").strip()
        state = request.form.get("state", "").strip()
        pincode = request.form.get("pincode", "").strip()
        try:
            gst_rate = float(request.form.get("gst_rate", "0"))
        except (ValueError, TypeError):
            gst_rate = -1

        if not name or not 0 <= gst_rate <= 100:
            flash("Enter a business name and GST rate between 0 and 100.")
            return redirect(url_for("business_settings"))
        if gstin and (len(gstin) != 15 or not gstin.isalnum()):
            flash("GSTIN must be 15 alphanumeric characters or blank.")
            return redirect(url_for("business_settings"))
        if pincode and (len(pincode) != 6 or not pincode.isdigit()):
            flash("PIN code must be 6 digits or blank.")
            return redirect(url_for("business_settings"))

        with connect() as c:
            c.execute("""
                UPDATE businesses SET name=?,gstin=?,address=?,city=?,
                state=?,pincode=?,gst_rate=? WHERE id=?
            """, (name, gstin, address, city, state, pincode, gst_rate, bid))
        flash("Business settings saved.")
        return redirect(url_for("business_settings"))

    with connect() as c:
        business = c.execute("SELECT * FROM businesses WHERE id=?", (bid,)).fetchone()

    return page("Business Settings", """
    <div class="card"><h2>Business Settings</h2><form method="post">
    <label>Business name</label><input name="name" value="{{ business.name }}" required>
    <label>GSTIN</label><input name="gstin" maxlength="15" value="{{ business.gstin or '' }}">
    <label>Address</label><input name="address" value="{{ business.address or '' }}">
    <div class="grid">
    <div><label>City</label><input name="city" value="{{ business.city or '' }}"></div>
    <div><label>State</label><input name="state" value="{{ business.state or '' }}"></div>
    <div><label>PIN code</label><input name="pincode" maxlength="6" value="{{ business.pincode or '' }}"></div>
    <div><label>GST rate (%)</label><input type="number" name="gst_rate" min="0" max="100" step="0.01" value="{{ business.gst_rate or 0 }}"></div>
    </div><button>Save Settings</button></form>
    <small>This applies one GST rate to the whole bill. Verify the applicable GST treatment for your products.</small></div>
    """, business=business)


def ensure_categories(bid):
    with connect() as c:
        biz = c.execute("SELECT category FROM businesses WHERE id=?", (bid,)).fetchone()
        allowed = CATEGORY_MAP.get(biz["category"] if biz else "", ["Other Products"])
        for cat in allowed:
            c.execute("INSERT OR IGNORE INTO categories(business_id,name) VALUES(?,?)", (bid, cat))
        cats = c.execute(
            "SELECT * FROM categories WHERE business_id=? ORDER BY name", (bid,)
        ).fetchall()
    return allowed, cats


@app.route("/products", methods=["GET", "POST"])
@owner_required
def products():
    bid = session["business_id"]
    allowed, cats = ensure_categories(bid)

    if request.method == "POST":
        name = request.form.get("name", "").strip()
        category_id = request.form.get("category_id", type=int)
        price = request.form.get("price", type=float)
        stock = request.form.get("stock", type=int)
        with connect() as c:
            valid = c.execute(
                "SELECT id FROM categories WHERE id=? AND business_id=?",
                (category_id, bid)
            ).fetchone() if category_id else None
            if not name or not valid or price is None or not math.isfinite(price) or price < 0 or stock is None or stock < 0:
                flash("Check product name, category, price and stock.")
                return redirect(url_for("products"))
            c.execute(
                "INSERT INTO products(business_id,category_id,name,price,stock) VALUES(?,?,?,?,?)",
                (bid, category_id, name, price, stock)
            )
        flash("Product added.")
        return redirect(url_for("products"))

    with connect() as c:
        show_hidden = request.args.get("show_hidden", "0") == "1"
        if show_hidden:
            rows = c.execute("""
                SELECT p.*,cat.name category_name,
                  (NOT EXISTS (SELECT 1 FROM sale_items si WHERE si.product_id=p.id)
                   AND NOT EXISTS (SELECT 1 FROM stock_adjustments sa WHERE sa.product_id=p.id)) AS can_delete
                FROM products p JOIN categories cat ON cat.id=p.category_id
                WHERE p.business_id=? ORDER BY p.active DESC, p.id DESC
            """, (bid,)).fetchall()
        else:
            rows = c.execute("""
                SELECT p.*,cat.name category_name,
                  (NOT EXISTS (SELECT 1 FROM sale_items si WHERE si.product_id=p.id)
                   AND NOT EXISTS (SELECT 1 FROM stock_adjustments sa WHERE sa.product_id=p.id)) AS can_delete
                FROM products p JOIN categories cat ON cat.id=p.category_id
                WHERE p.business_id=? AND p.active=1 ORDER BY p.id DESC
            """, (bid,)).fetchall()
        adjustments = c.execute("""
            SELECT sa.*, p.name product_name, u.username adjusted_by
            FROM stock_adjustments sa
            JOIN products p ON p.id=sa.product_id AND p.business_id=sa.business_id
            LEFT JOIN users u ON u.id=sa.user_id
            WHERE sa.business_id=? ORDER BY sa.id DESC LIMIT 30
        """, (bid,)).fetchall()

    return page("Products", """
    <div class="card"><h2>Add Product</h2><form method="post"><div class="grid">
    <div><label>Product name</label><input name="name" required></div>
    <div><label>Category</label><select name="category_id" required>
    {% for cat in cats %}<option value="{{ cat.id }}">{{ cat.name }}</option>{% endfor %}
    </select></div>
    <div><label>Price</label><input type="number" name="price" min="0" step="0.01" required></div>
    <div><label>Opening stock</label><input type="number" name="stock" min="0" step="1" required></div>
    </div><button>Add Product</button></form></div>

    <div class="card"><h2>Import Products from CSV</h2>
    <p>Required columns: product_name, category, price, stock</p>
    <a class="btn" href="{{ url_for('sample_products_csv') }}">Download Sample CSV</a>
    <form method="post" action="{{ url_for('import_products') }}" enctype="multipart/form-data">
    <input type="file" name="csv_file" accept=".csv,text/csv" required>
    <button>Import Products</button></form></div>

    <div class="card"><h2>Product List & Stock Adjustment</h2>
    <p>Use Increase to add missing stock or Reduce to correct excess stock. Stock cannot go below zero.</p>
    <p>{% if show_hidden %}<a class="btn" href="{{ url_for('products') }}">Hide inactive products</a>{% else %}<a class="btn" href="{{ url_for('products', show_hidden=1) }}">Show hidden products</a>{% endif %}</p>
    <div class="tablewrap"><table>
    <tr><th>Name</th><th>Category</th><th>Price</th><th>Current Stock</th><th>Status</th><th>Adjust Stock</th><th>Visibility</th></tr>
    {% for p in rows %}<tr><td>{{ p.name }}</td><td>{{ p.category_name }}</td>
    <td>₹{{ "%.2f"|format(p.price) }}</td><td><strong>{{ p.stock }}</strong></td><td>{{ 'Active' if p.active else 'Hidden' }}</td><td>
    <form method="post" action="{{ url_for('adjust_product_stock', product_id=p.id) }}">
    <select name="adjustment_type" aria-label="Adjustment type for {{ p.name }}" required>
      <option value="increase">Increase (+)</option><option value="decrease">Reduce (−)</option>
    </select>
    <input type="number" name="quantity" min="1" step="1" placeholder="Quantity" required>
    <input name="reason" maxlength="200" placeholder="Reason (optional)">
    <button {% if not p.active %}disabled title="Unhide this product before adjusting stock"{% endif %}>Update Stock</button></form></td><td>
    <form method="post" action="{{ url_for('toggle_product_visibility', product_id=p.id) }}">
    <input type="hidden" name="show_hidden" value="{{ 1 if show_hidden else 0 }}">
    {% if p.active %}<button type="submit" class="danger" onclick="return confirm('Hide this product? It will no longer be available for new bills. Existing sales history will be kept.');">Hide Product</button>
    {% else %}<button type="submit">Unhide Product</button>{% endif %}
    </form>
    {% if p.can_delete %}<form method="post" action="{{ url_for('delete_product', product_id=p.id) }}" onsubmit="return confirm('Permanently delete this product? This cannot be undone.');"><button type="submit" class="danger">Delete Permanently</button></form>
    {% else %}<small>Permanent delete unavailable: this product has sales or stock-adjustment history. Hide it instead.</small>{% endif %}
    </td></tr>
    {% else %}<tr><td colspan="7">No products.</td></tr>{% endfor %}
    </table></div></div>

    <div class="card"><h2>Recent Stock Adjustments</h2><div class="tablewrap"><table>
    <tr><th>Date</th><th>Product</th><th>Change</th><th>Before</th><th>After</th><th>Reason</th><th>Adjusted By</th></tr>
    {% for a in adjustments %}<tr><td>{{ a.created_at }}</td><td>{{ a.product_name }}</td>
    <td>{{ '+' if a.adjustment_type == 'increase' else '−' }}{{ a.quantity }}</td>
    <td>{{ a.old_stock }}</td><td>{{ a.new_stock }}</td><td>{{ a.reason or '—' }}</td>
    <td>{{ a.adjusted_by or 'Unknown user' }}</td></tr>
    {% else %}<tr><td colspan="7">No stock adjustments yet.</td></tr>{% endfor %}
    </table></div></div>
    """, cats=cats, rows=rows, adjustments=adjustments, show_hidden=show_hidden)


@app.route("/products/<int:product_id>/visibility", methods=["POST"])
@owner_required
def toggle_product_visibility(product_id):
    bid = session["business_id"]
    show_hidden = request.form.get("show_hidden", "0") == "1"
    with connect() as c:
        product = c.execute(
            "SELECT id,name,active FROM products WHERE id=? AND business_id=? FOR UPDATE",
            (product_id, bid)
        ).fetchone()
        if not product:
            flash("Product not found in this business.")
            return redirect(url_for("products", show_hidden=1 if show_hidden else 0))
        new_active = 0 if int(product["active"] or 0) else 1
        c.execute(
            "UPDATE products SET active=? WHERE id=? AND business_id=?",
            (new_active, product_id, bid)
        )
    flash(f"Product '{product['name']}' {'is now available' if new_active else 'has been hidden'}. " +
          ("It can be used in new bills." if new_active else "It will not appear in new bills; past sales records are preserved."))
    return redirect(url_for("products", show_hidden=1 if show_hidden or not new_active else 0))


@app.route("/products/<int:product_id>/delete", methods=["POST"])
@owner_required
def delete_product(product_id):
    bid = session["business_id"]
    with connect() as c:
        product = c.execute(
            "SELECT id,name FROM products WHERE id=? AND business_id=? FOR UPDATE",
            (product_id, bid)
        ).fetchone()
        if not product:
            flash("Product not found in this business.")
            return redirect(url_for("products"))
        has_sales = c.execute(
            "SELECT 1 AS found FROM sale_items WHERE product_id=? LIMIT 1", (product_id,)
        ).fetchone()
        has_adjustments = c.execute(
            "SELECT 1 AS found FROM stock_adjustments WHERE product_id=? LIMIT 1", (product_id,)
        ).fetchone()
        if has_sales or has_adjustments:
            flash("This product has sales or stock-adjustment history. Hide it instead to preserve records.")
            return redirect(url_for("products", show_hidden=1))
        c.execute("DELETE FROM products WHERE id=? AND business_id=?", (product_id, bid))
    flash(f"Product '{product['name']}' permanently deleted.")
    return redirect(url_for("products"))


@app.route("/products/<int:product_id>/adjust-stock", methods=["POST"])
@owner_required
def adjust_product_stock(product_id):
    bid = session["business_id"]
    adjustment_type = request.form.get("adjustment_type", "").strip().lower()
    quantity = request.form.get("quantity", type=int)
    reason = request.form.get("reason", "").strip()[:200]

    if adjustment_type not in ("increase", "decrease") or quantity is None or quantity < 1:
        flash("Choose Increase or Reduce and enter a quantity of at least 1.")
        return redirect(url_for("products"))

    with connect() as c:
        product = c.execute(
            "SELECT id,name,stock,active FROM products WHERE id=? AND business_id=? FOR UPDATE",
            (product_id, bid)
        ).fetchone()
        if not product:
            flash("Product not found in this business.")
            return redirect(url_for("products"))
        if not int(product["active"] or 0):
            flash("Unhide this product before adjusting its stock.")
            return redirect(url_for("products", show_hidden=1))

        old_stock = int(product["stock"])
        if adjustment_type == "decrease" and quantity > old_stock:
            flash(f"Cannot reduce {product['name']} by {quantity}; current stock is only {old_stock}.")
            return redirect(url_for("products"))

        new_stock = old_stock + quantity if adjustment_type == "increase" else old_stock - quantity
        c.execute("UPDATE products SET stock=? WHERE id=? AND business_id=?", (new_stock, product_id, bid))
        c.execute("""
            INSERT INTO stock_adjustments
            (business_id,product_id,user_id,adjustment_type,quantity,old_stock,new_stock,reason,created_at)
            VALUES(?,?,?,?,?,?,?,?,?)
        """, (bid, product_id, session.get("user_id"), adjustment_type, quantity,
              old_stock, new_stock, reason, now_text()))

    flash(f"Stock updated for {product['name']}: {old_stock} → {new_stock}.")
    return redirect(url_for("products"))

@app.route("/products/sample.csv")
@owner_required
def sample_products_csv():
    bid = session["business_id"]
    allowed, _ = ensure_categories(bid)
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["product_name", "category", "price", "stock"])
    for i, cat in enumerate(allowed[:2], 1):
        writer.writerow([f"Example Product {i}", cat, "100.00", 10])
    response = make_response("\ufeff" + output.getvalue())
    response.headers["Content-Type"] = "text/csv; charset=utf-8"
    response.headers["Content-Disposition"] = 'attachment; filename="sample_products.csv"'
    return response


@app.route("/products/import", methods=["POST"])
@owner_required
def import_products():
    bid = session["business_id"]
    uploaded = request.files.get("csv_file")
    if not uploaded or not uploaded.filename or not uploaded.filename.lower().endswith(".csv"):
        flash("Select a CSV file.")
        return redirect(url_for("products"))

    try:
        reader = csv.DictReader(io.StringIO(uploaded.read().decode("utf-8-sig")))
        headers = {str(h).strip().lower() for h in (reader.fieldnames or []) if h}
        required = {"product_name", "category", "price", "stock"}
        if not required.issubset(headers):
            flash("CSV must contain product_name, category, price, stock.")
            return redirect(url_for("products"))

        header_map = {str(h).strip().lower(): h for h in reader.fieldnames if h}
        allowed, _ = ensure_categories(bid)
        valid_rows, errors = [], []

        with connect() as c:
            category_rows = c.execute(
                "SELECT id,name FROM categories WHERE business_id=?", (bid,)
            ).fetchall()
            category_ids = {r["name"]: r["id"] for r in category_rows if r["name"] in allowed}

            for line, row in enumerate(reader, 2):
                if not any(str(v or "").strip() for v in row.values()):
                    continue
                try:
                    name = str(row.get(header_map["product_name"], "") or "").strip()
                    category = str(row.get(header_map["category"], "") or "").strip()
                    price = float(row.get(header_map["price"], ""))
                    stock_float = float(row.get(header_map["stock"], ""))
                    if not name:
                        raise ValueError("Product name required")
                    if category not in category_ids:
                        raise ValueError("Invalid category")
                    if not math.isfinite(price) or price < 0:
                        raise ValueError("Invalid price")
                    if not math.isfinite(stock_float) or stock_float < 0 or not stock_float.is_integer():
                        raise ValueError("Stock must be a non-negative whole number")
                    valid_rows.append((bid, category_ids[category], name, price, int(stock_float)))
                except (ValueError, TypeError, KeyError) as exc:
                    errors.append(f"Row {line}: {exc}")
                if len(valid_rows) + len(errors) > 5000:
                    errors.append("Maximum 5000 rows per import.")
                    valid_rows = []
                    break

            if errors:
                flash("Import cancelled: " + " | ".join(errors[:8]))
                return redirect(url_for("products"))
            if not valid_rows:
                flash("No product rows found.")
                return redirect(url_for("products"))
            c.executemany(
                "INSERT INTO products(business_id,category_id,name,price,stock) VALUES(?,?,?,?,?)",
                valid_rows
            )

        flash(f"Imported {len(valid_rows)} products.")
    except (UnicodeDecodeError, csv.Error):
        flash("Could not read this CSV file.")
    except Exception:
        app.logger.exception("Product CSV import failed")
        flash("Import failed. Check the application console.")

    return redirect(url_for("products"))


@app.route("/customers", methods=["GET", "POST"])
@owner_required
def customers():
    bid = session["business_id"]
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        phone = request.form.get("phone", "").strip()
        username = request.form.get("username", "").strip() or phone
        password = request.form.get("password", "")
        if not name or not phone or not username or len(password) < 6:
            flash("Enter customer name, phone, username and password (minimum 6 characters).")
            return redirect(url_for("customers"))
        try:
            with connect() as c:
                c.execute("""
                    INSERT INTO customers(business_id,name,phone,username,password_hash,created_at)
                    VALUES(?,?,?,?,?,?)
                """, (bid, name, phone, username, generate_password_hash(password), now_text()))
            flash("Customer account created.")
        except psycopg2.IntegrityError:
            flash("Phone number or username already exists.")
        return redirect(url_for("customers"))

    search = request.args.get("phone", "").strip()
    with connect() as c:
        if search:
            rows = c.execute(
                "SELECT * FROM customers WHERE business_id=? AND phone LIKE ? ORDER BY name",
                (bid, "%" + search + "%")
            ).fetchall()
        else:
            rows = c.execute(
                "SELECT * FROM customers WHERE business_id=? ORDER BY id DESC", (bid,)
            ).fetchall()

    offer = request.args.get("offer", "Hello! We have special offers and discounts for you. Visit us today!")
    return page("Customers", """
    <div class="card"><h2>Create Customer Login</h2><form method="post"><div class="grid">
    <div><label>Name</label><input name="name" required></div>
    <div><label>Phone</label><input name="phone" required></div>
    <div><label>Username (defaults to phone)</label><input name="username"></div>
    <div><label>Password (minimum 6 characters)</label><input type="password" name="password" minlength="6" required></div>
    </div><button>Create Customer</button></form></div>
    <div class="card"><h2>Search Customer</h2><form method="get">
    <input name="phone" value="{{ search }}" placeholder="Phone number">
    <button>Search</button><a class="btn" href="{{ url_for('customers') }}">Show All</a></form></div>
    <div class="card"><h2>Customers and WhatsApp Offers</h2>
    <form method="get"><input name="phone" value="{{ search }}" placeholder="Phone filter">
    <textarea name="offer" rows="3">{{ offer }}</textarea><button>Apply Filter / Offer</button></form>
    <div class="tablewrap"><table><tr><th>Name</th><th>Phone</th><th>Username</th><th>Points</th><th>WhatsApp</th></tr>
    {% for x in rows %}<tr><td>{{ x.name }}</td><td>{{ x.phone }}</td><td>{{ x.username or '-' }}</td><td>{{ x.points }}</td>
    <td><a class="btn whatsapp" href="{{ url_for('whatsapp_offer', customer_id=x.id, offer=offer) }}">Send Offer</a></td></tr>
    {% else %}<tr><td colspan="5">No customers.</td></tr>{% endfor %}
    </table></div></div>
    """, rows=rows, search=search, offer=offer)


@app.route("/customers/whatsapp/<int:customer_id>")
@owner_required
def whatsapp_offer(customer_id):
    bid = session["business_id"]
    offer = request.args.get("offer", "We have special offers and discounts for you. Visit us today!")
    with connect() as c:
        cust = c.execute(
            "SELECT * FROM customers WHERE id=? AND business_id=?", (customer_id, bid)
        ).fetchone()
    if not cust:
        flash("Customer not found.")
        return redirect(url_for("customers"))
    digits = "".join(ch for ch in cust["phone"] if ch.isdigit())
    if not digits:
        flash("Invalid customer phone number.")
        return redirect(url_for("customers"))
    return redirect("https://wa.me/" + digits + "?text=" + quote(f"Hello {cust['name']}! {offer}"))


@app.route("/billing", methods=["GET", "POST"])
@billing_access_required
def billing():
    bid = session["business_id"]
    phone_search = request.values.get("phone_search", "").strip()
    matched_customer = None

    with connect() as c:
        products_list = c.execute(
            "SELECT * FROM products WHERE business_id=? AND active=1 AND stock>0 ORDER BY name", (bid,)
        ).fetchall()
        customers_list = c.execute(
            "SELECT * FROM customers WHERE business_id=? ORDER BY name", (bid,)
        ).fetchall()
        if phone_search:
            matched_customer = c.execute(
                "SELECT * FROM customers WHERE business_id=? AND phone LIKE ? ORDER BY id DESC LIMIT 1",
                (bid, "%" + phone_search + "%")
            ).fetchone()

    if request.method == "POST":
        product_ids = request.form.getlist("product_id[]")
        quantities = request.form.getlist("quantity[]")
        customer_id = request.form.get("customer_id", type=int)
        redeem_requested = request.form.get("redeem", type=int) or 0

        if redeem_requested < 0:
            flash("Invalid loyalty points.")
            return redirect(url_for("billing"))
        if not product_ids or len(product_ids) != len(quantities):
            flash("Add at least one product.")
            return redirect(url_for("billing"))

        cart = {}
        try:
            for pid, qty in zip(product_ids, quantities):
                pid, qty = int(pid), int(qty)
                if pid <= 0 or qty <= 0:
                    raise ValueError
                cart[pid] = cart.get(pid, 0) + qty
        except (ValueError, TypeError):
            flash("Check product selections and quantities.")
            return redirect(url_for("billing"))

        try:
            # BEGIN IMMEDIATE serializes stock/points updates to reduce double-selling.
            with connect() as c:
                c.execute("BEGIN IMMEDIATE")
                customer = c.execute(
                    "SELECT * FROM customers WHERE id=? AND business_id=?",
                    (customer_id, bid)
                ).fetchone() if customer_id else None
                if customer_id and not customer:
                    raise ValueError("Invalid customer.")

                business = c.execute(
                    "SELECT * FROM businesses WHERE id=?", (bid,)
                ).fetchone()
                gst_rate = float(business["gst_rate"] or 0) if business else 0.0

                cart_items, subtotal = [], 0.0
                for pid, qty in cart.items():
                    product = c.execute(
                        "SELECT * FROM products WHERE id=? AND business_id=? AND active=1",
                        (pid, bid)
                    ).fetchone()
                    if not product:
                        raise ValueError("A selected product was not found.")
                    if qty > product["stock"]:
                        raise ValueError(
                            f"Insufficient stock for {product['name']}. Available: {product['stock']}."
                        )
                    line_total = round(float(product["price"]) * qty, 2)
                    subtotal = round(subtotal + line_total, 2)
                    cart_items.append((product, qty, line_total))

                available_points = int(customer["points"]) if customer else 0
                redeem_points = min(redeem_requested, available_points)
                gst_amount = round(subtotal * gst_rate / 100, 2)
                before_redemption = round(subtotal + gst_amount, 2)
                redeem_value = min(round(redeem_points * 0.10, 2), before_redemption)
                points_used = min(redeem_points, int(round(redeem_value * 100)) // 10)
                total = round(before_redemption - points_used * 0.10, 2)
                points_earned = int(total // 100) * 10

                cur = c.execute("""
                    INSERT INTO sales(business_id,customer_id,customer_name,customer_phone,
                    total,subtotal,points_earned,points_redeemed,created_at,gst_rate,gst_amount)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    bid, customer["id"] if customer else None,
                    customer["name"] if customer else "Walk-in Customer",
                    customer["phone"] if customer else "",
                    total, subtotal, points_earned, points_used, now_text(),
                    gst_rate, gst_amount
                ))
                sale_id = cur.lastrowid

                for product, qty, line_total in cart_items:
                    c.execute("""
                        INSERT INTO sale_items(sale_id,product_id,product_name,quantity,unit_price,line_total)
                        VALUES(?,?,?,?,?,?)
                    """, (sale_id, product["id"], product["name"], qty, product["price"], line_total))
                    c.execute(
                        "UPDATE products SET stock=stock-? WHERE id=? AND business_id=? AND stock>=?",
                        (qty, product["id"], bid, qty)
                    )
                    if c.execute("SELECT changes()").fetchone()["changes"] != 1:
                        raise ValueError(f"Stock changed for {product['name']}. Please try again.")

                if customer:
                    c.execute(
                        "UPDATE customers SET points=points-?+? WHERE id=? AND business_id=? AND points>=?",
                        (points_used, points_earned, customer["id"], bid, points_used)
                    )
                    if c.execute("SELECT changes()").fetchone()["changes"] != 1:
                        raise ValueError("Customer points changed. Please retry the bill.")

        except ValueError as exc:
            flash(str(exc))
            return redirect(url_for("billing"))
        except psycopg2.Error:
            app.logger.exception("Bill creation failed")
            flash("Bill could not be saved. Check the database and try again.")
            return redirect(url_for("billing"))

        flash(f"Bill #{sale_id} generated. Total ₹{total:.2f}. Points earned: {points_earned}.")
        return redirect(url_for("bill_detail", sale_id=sale_id))

    return page("Billing", """
    <div class="card"><h2>Find Customer by Phone</h2><form method="get">
    <input name="phone_search" value="{{ phone_search }}" placeholder="Enter full or partial phone number">
    <button>Search Customer</button></form>
    {% if phone_search %}{% if matched_customer %}
    <p>Found: <b>{{ matched_customer.name }}</b> — {{ matched_customer.phone }} — {{ matched_customer.points }} points</p>
    {% else %}<p>No customer found. You can still use walk-in billing.</p>{% endif %}{% endif %}
    </div>

    <div class="card"><h2>Create Bill</h2><form method="post" id="billForm">
    <input type="hidden" name="phone_search" value="{{ phone_search }}">
    <label>Customer</label><select name="customer_id">
    <option value="">Walk-in customer</option>
    {% for x in customers_list %}<option value="{{ x.id }}" {% if matched_customer and matched_customer.id==x.id %}selected{% endif %}>
    {{ x.name }} — {{ x.phone }} — {{ x.points }} points</option>{% endfor %}
    </select>
    <h3>Products</h3><div id="cartRows"></div>
    <button type="button" onclick="addRow()">+ Add Product</button>
    <label>Loyalty points to redeem</label><input type="number" name="redeem" min="0" value="0">
    <h3>Estimated subtotal: ₹<span id="subtotal">0.00</span></h3>
    <button type="submit">Generate Bill</button></form></div>

    <script>
    const products = [
    {% for p in products_list %}
    {id:{{ p.id }},name:{{ p.name|tojson }},price:{{ p.price }},stock:{{ p.stock }}},
    {% endfor %}];

    function addRow(){
      const container=document.getElementById("cartRows");
      const row=document.createElement("div"); row.className="card cart-row";
      const label=document.createElement("label"); label.textContent="Product"; row.appendChild(label);
      const select=document.createElement("select"); select.name="product_id[]"; select.required=true;
      const blank=document.createElement("option"); blank.value=""; blank.textContent="Select product"; select.appendChild(blank);
      products.forEach(p=>{const o=document.createElement("option");o.value=p.id;
        o.textContent=p.name+" — ₹"+Number(p.price).toFixed(2)+" — Stock "+p.stock;
        o.dataset.price=p.price;o.dataset.stock=p.stock;select.appendChild(o);});
      row.appendChild(select);
      const ql=document.createElement("label");ql.textContent="Quantity";row.appendChild(ql);
      const qty=document.createElement("input");qty.type="number";qty.name="quantity[]";qty.value="1";qty.min="1";qty.required=true;row.appendChild(qty);
      const amount=document.createElement("p");amount.textContent="Line total: ₹0.00";row.appendChild(amount);
      const remove=document.createElement("button");remove.type="button";remove.className="red";remove.textContent="Remove";
      remove.onclick=()=>{row.remove();updateTotal();};row.appendChild(remove);
      select.addEventListener("change",updateTotal);qty.addEventListener("input",updateTotal);
      container.appendChild(row);updateTotal();
    }
    function updateTotal(){
      let total=0;
      document.querySelectorAll(".cart-row").forEach(row=>{
        const s=row.querySelector('select[name="product_id[]"]');
        const q=row.querySelector('input[name="quantity[]"]');
        const o=s.options[s.selectedIndex];const price=Number(o&&o.dataset.price||0);
        const line=price*Math.max(0,Number(q.value||0));
        row.querySelector("p").textContent="Line total: ₹"+line.toFixed(2);total+=line;
      });
      document.getElementById("subtotal").textContent=total.toFixed(2);
    }
    addRow();
    document.getElementById("billForm").addEventListener("submit",function(e){
      const rows=document.querySelectorAll(".cart-row");
      if(!rows.length){e.preventDefault();alert("Add a product.");return;}
      for(const row of rows){
        const s=row.querySelector('select[name="product_id[]"]');
        const q=row.querySelector('input[name="quantity[]"]');
        const o=s.options[s.selectedIndex];
        if(!s.value||!q.value||Number(q.value)<1){e.preventDefault();alert("Select product and valid quantity.");return;}
        if(Number(q.value)>Number(o.dataset.stock)){e.preventDefault();alert("Quantity exceeds stock.");return;}
      }
    });
    </script>
    """, products_list=products_list, customers_list=customers_list,
       phone_search=phone_search, matched_customer=matched_customer)


def load_bill(sale_id, bid):
    with connect() as c:
        sale = c.execute(
            "SELECT * FROM sales WHERE id=? AND business_id=?", (sale_id, bid)
        ).fetchone()
        if not sale:
            return None, None, None
        items = c.execute("SELECT * FROM sale_items WHERE sale_id=? ORDER BY id", (sale_id,)).fetchall()
        business = c.execute("SELECT * FROM businesses WHERE id=?", (bid,)).fetchone()
    return sale, items, business


@app.route("/bill/<int:sale_id>")
@billing_access_required
def bill_detail(sale_id):
    sale, items, business = load_bill(sale_id, session["business_id"])
    if not sale:
        flash("Bill not found.")
        return redirect(url_for("billing"))
    return page("Bill", """
    <div class="card"><h2>{{ business.name }}</h2>
    <p>{{ business.address or '' }} {{ business.city or '' }} {{ business.state or '' }} {{ business.pincode or '' }}</p>
    {% if business.gstin %}<p>GSTIN: {{ business.gstin }}</p>{% endif %}
    <h3>Bill #{{ sale.id }}</h3><p>Date: {{ sale.created_at }}</p>
    <p>Customer: {{ sale.customer_name }} | {{ sale.customer_phone or '-' }}</p>
    <div class="tablewrap"><table><tr><th>Product</th><th>Qty</th><th>Rate</th><th>Total</th></tr>
    {% for x in items %}<tr><td>{{ x.product_name }}</td><td>{{ x.quantity }}</td>
    <td>₹{{ "%.2f"|format(x.unit_price) }}</td><td>₹{{ "%.2f"|format(x.line_total) }}</td></tr>{% endfor %}
    </table></div>
    <p>Subtotal: ₹{{ "%.2f"|format(sale.subtotal) }}</p>
    <p>GST ({{ "%.2f"|format(sale.gst_rate or 0) }}%): ₹{{ "%.2f"|format(sale.gst_amount or 0) }}</p>
    <p>Points redeemed: {{ sale.points_redeemed }} (₹{{ "%.2f"|format(sale.points_redeemed*0.10) }})</p>
    <h2>Grand Total: ₹{{ "%.2f"|format(sale.total) }}</h2>
    <p>Points earned: {{ sale.points_earned }}</p>
    <a class="btn" href="{{ url_for('bill_pdf', sale_id=sale.id) }}">Download PDF</a>
    <button onclick="window.print()">Print</button>
    <a class="btn" href="{{ url_for('billing') }}">New Bill</a></div>
    """, sale=sale, items=items, business=business)


@app.route("/bill/<int:sale_id>/pdf")
@billing_access_required
def bill_pdf(sale_id):
    if not PDF_AVAILABLE:
        return "PDF library missing. Install reportlab.", 500
    sale, items, business = load_bill(sale_id, session["business_id"])
    if not sale or not business:
        return "Bill not found.", 404

    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4, rightMargin=35, leftMargin=35, topMargin=35, bottomMargin=35)
    styles = getSampleStyleSheet()
    story = [
        Paragraph(str(business["name"]), styles["Title"]),
        Paragraph("SALES RECEIPT / TAX INVOICE", styles["Heading2"]),
        Spacer(1, 8)
    ]
    address = ", ".join(x for x in [
        business["address"] or "", business["city"] or "",
        business["state"] or "", business["pincode"] or ""
    ] if x)
    if address:
        story.append(Paragraph(address, styles["Normal"]))
    if business["gstin"]:
        story.append(Paragraph("GSTIN: " + str(business["gstin"]), styles["Normal"]))
    story.extend([
        Spacer(1, 10),
        Paragraph(f"Bill No: {sale['id']}", styles["Normal"]),
        Paragraph(f"Date: {sale['created_at']}", styles["Normal"]),
        Paragraph(f"Customer: {sale['customer_name']}", styles["Normal"]),
        Paragraph(f"Phone: {sale['customer_phone'] or '-'}", styles["Normal"]),
        Spacer(1, 12)
    ])
    data = [["Product", "Qty", "Rate (INR)", "Amount (INR)"]]
    for item in items:
        data.append([str(item["product_name"]), str(item["quantity"]),
                     f"{item['unit_price']:.2f}", f"{item['line_total']:.2f}"])
    data.extend([
        ["", "", "Subtotal", f"{sale['subtotal']:.2f}"],
        ["", "", f"GST ({sale['gst_rate']:.2f}%)", f"{sale['gst_amount']:.2f}"],
        ["", "", "Points redeemed", f"{sale['points_redeemed']} (INR {sale['points_redeemed']*0.10:.2f})"],
        ["", "", "GRAND TOTAL", f"{sale['total']:.2f}"],
        ["", "", "Points earned", str(sale["points_earned"])]
    ])
    table = Table(data, colWidths=[245, 45, 90, 100], repeatRows=1)
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#14532d")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.lightgrey),
        ("PADDING", (0, 0), (-1, -1), 7),
        ("ALIGN", (1, 1), (-1, -1), "RIGHT"),
        ("BACKGROUND", (0, -2), (-1, -2), colors.HexColor("#dcfce7")),
        ("FONTNAME", (0, -2), (-1, -2), "Helvetica-Bold")
    ]))
    story.extend([table, Spacer(1, 18), Paragraph("Thank you for your purchase!", styles["Italic"])])
    doc.build(story)
    response = make_response(buffer.getvalue())
    response.headers["Content-Type"] = "application/pdf"
    response.headers["Content-Disposition"] = f'attachment; filename="bill_{sale_id}.pdf"'
    return response


def report_filters(bid):
    start = request.args.get("start_date", "").strip()
    end = request.args.get("end_date", "").strip()
    customer = request.args.get("customer_id", "").strip()
    conditions = ["s.business_id=?"]
    params = [bid]
    if start:
        conditions.append("s.created_at>=?")
        params.append(start + " 00:00:00")
    if end:
        conditions.append("s.created_at<=?")
        params.append(end + " 23:59:59")
    if customer:
        try:
            conditions.append("s.customer_id=?")
            params.append(int(customer))
        except ValueError:
            raise ValueError("Invalid customer filter.")
    return start, end, customer, " AND ".join(conditions), params


@app.route("/reports")
@owner_required
def reports():
    bid = session["business_id"]
    try:
        start, end, customer_id, where_sql, params = report_filters(bid)
    except ValueError:
        flash("Invalid customer filter.")
        return redirect(url_for("reports"))

    with connect() as c:
        customers_list = c.execute(
            "SELECT id,name,phone FROM customers WHERE business_id=? ORDER BY name", (bid,)
        ).fetchall()
        sales = c.execute(
            f"SELECT * FROM sales s WHERE {where_sql} ORDER BY s.id DESC", params
        ).fetchall()
        summary = c.execute(f"""
            SELECT COUNT(*) n,COALESCE(SUM(s.total),0) total,
            COALESCE(SUM(s.points_earned),0) earned,
            COALESCE(SUM(s.points_redeemed),0) redeemed
            FROM sales s WHERE {where_sql}
        """, params).fetchone()

    return page("Reports", """
    <div class="card"><h2>Sales Reports</h2><form method="get"><div class="grid">
    <div><label>From date</label><input type="date" name="start_date" value="{{ start }}"></div>
    <div><label>To date</label><input type="date" name="end_date" value="{{ end }}"></div>
    <div><label>Customer</label><select name="customer_id"><option value="">All customers</option>
    {% for x in customers_list %}<option value="{{ x.id }}" {% if customer_id|string==x.id|string %}selected{% endif %}>{{ x.name }} — {{ x.phone }}</option>{% endfor %}
    </select></div></div><button>Apply Filters</button><a class="btn" href="{{ url_for('reports') }}">Reset</a></form>
    <hr><p>Bills: {{ summary.n }}</p><h3>Total sales: ₹{{ "%.2f"|format(summary.total) }}</h3>
    <p>Points earned: {{ summary.earned }} | Redeemed: {{ summary.redeemed }}</p>
    <a class="btn" href="{{ url_for('export_csv',start_date=start,end_date=end,customer_id=customer_id) }}">Download Sales CSV</a>
    <a class="btn" href="{{ url_for('loyalty_accounts') }}">Loyalty Accounts</a>
    </div>
    <div class="card"><h2>Bill History</h2><div class="tablewrap"><table>
    <tr><th>Bill</th><th>Date</th><th>Customer</th><th>Phone</th><th>Total</th><th>PDF</th></tr>
    {% for s in sales %}<tr><td>{{ s.id }}</td><td>{{ s.created_at }}</td><td>{{ s.customer_name }}</td>
    <td>{{ s.customer_phone or '-' }}</td><td>₹{{ "%.2f"|format(s.total) }}</td>
    <td><a href="{{ url_for('bill_pdf',sale_id=s.id) }}">PDF</a></td></tr>
    {% else %}<tr><td colspan="6">No sales found.</td></tr>{% endfor %}
    </table></div></div>
    """, sales=sales, summary=summary, customers_list=customers_list,
       start=start, end=end, customer_id=customer_id)


@app.route("/export.csv")
@owner_required
def export_csv():
    bid = session["business_id"]
    try:
        start, end, customer_id, where_sql, params = report_filters(bid)
    except ValueError:
        flash("Invalid customer filter.")
        return redirect(url_for("reports"))

    with connect() as c:
        rows = c.execute(f"""
            SELECT s.id bill_id,s.created_at,s.customer_name,s.customer_phone,
            i.product_name,i.quantity,i.unit_price,i.line_total,s.subtotal,
            s.gst_rate,s.gst_amount,s.total,s.points_earned,s.points_redeemed
            FROM sales s JOIN sale_items i ON i.sale_id=s.id
            WHERE {where_sql} ORDER BY s.id DESC,i.id
        """, params).fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "Bill ID", "Date", "Customer", "Phone", "Product", "Quantity",
        "Unit Price", "Line Total", "Bill Subtotal", "GST Rate (%)",
        "GST Amount", "Bill Total", "Points Earned", "Points Redeemed"
    ])
    for r in rows:
        writer.writerow([
            r["bill_id"], r["created_at"], r["customer_name"], r["customer_phone"] or "",
            r["product_name"], r["quantity"], f"{r['unit_price']:.2f}",
            f"{r['line_total']:.2f}", f"{r['subtotal']:.2f}",
            f"{r['gst_rate']:.2f}", f"{r['gst_amount']:.2f}",
            f"{r['total']:.2f}", r["points_earned"], r["points_redeemed"]
        ])
    response = make_response("\ufeff" + output.getvalue())
    response.headers["Content-Type"] = "text/csv; charset=utf-8"
    response.headers["Content-Disposition"] = 'attachment; filename="sales_report.csv"'
    return response


@app.route("/loyalty")
@owner_required
def loyalty_accounts():
    bid = session["business_id"]
    start = request.args.get("start_date", "").strip()
    end = request.args.get("end_date", "").strip()
    customer_id = request.args.get("customer_id", "").strip()

    customer_conditions = ["business_id=?"]
    customer_params = [bid]
    sales_conditions = ["business_id=?"]
    sales_params = [bid]

    if customer_id:
        try:
            customer_conditions.append("id=?")
            customer_params.append(int(customer_id))
            sales_conditions.append("customer_id=?")
            sales_params.append(int(customer_id))
        except ValueError:
            flash("Invalid customer filter.")
            return redirect(url_for("loyalty_accounts"))
    if start:
        sales_conditions.append("created_at>=?")
        sales_params.append(start + " 00:00:00")
    if end:
        sales_conditions.append("created_at<=?")
        sales_params.append(end + " 23:59:59")

    with connect() as c:
        customers_list = c.execute(
            "SELECT id,name,phone FROM customers WHERE business_id=? ORDER BY name", (bid,)
        ).fetchall()
        balances = c.execute(
            f"SELECT id,name,phone,points FROM customers WHERE {' AND '.join(customer_conditions)} ORDER BY name",
            customer_params
        ).fetchall()
        totals = c.execute(
            f"""SELECT COALESCE(SUM(points_earned),0) earned,
            COALESCE(SUM(points_redeemed),0) redeemed,COUNT(*) bills
            FROM sales WHERE {' AND '.join(sales_conditions)}""", sales_params
        ).fetchone()
        history = c.execute(
            f"""SELECT id,created_at,customer_name,customer_phone,points_earned,
            points_redeemed,total FROM sales WHERE {' AND '.join(sales_conditions)}
            ORDER BY id DESC LIMIT 300""", sales_params
        ).fetchall()

    current_points = sum(x["points"] for x in balances)
    return page("Loyalty Accounts", """
    <div class="card"><h2>Loyalty Accounts</h2><form method="get"><div class="grid">
    <div><label>From date</label><input type="date" name="start_date" value="{{ start }}"></div>
    <div><label>To date</label><input type="date" name="end_date" value="{{ end }}"></div>
    <div><label>Customer</label><select name="customer_id"><option value="">All customers</option>
    {% for x in customers_list %}<option value="{{ x.id }}" {% if customer_id|string==x.id|string %}selected{% endif %}>{{ x.name }} — {{ x.phone }}</option>{% endfor %}
    </select></div></div><button>Apply Filters</button><a class="btn" href="{{ url_for('loyalty_accounts') }}">Reset</a></form>
    <p>Current points are today's balances; transaction totals use the selected dates.</p>
    </div>
    <div class="grid">
    <div class="card"><h3>Earned</h3><h2>{{ totals.earned }}</h2></div>
    <div class="card"><h3>Redeemed</h3><h2>{{ totals.redeemed }}</h2></div>
    <div class="card"><h3>Current Points</h3><h2>{{ current_points }}</h2></div>
    <div class="card"><h3>Redemption Value</h3><h2>₹{{ "%.2f"|format(current_points*0.10) }}</h2></div>
    </div>
    <div class="card"><h2>Customer Balances</h2><div class="tablewrap"><table>
    <tr><th>Name</th><th>Phone</th><th>Points</th><th>Value</th></tr>
    {% for x in balances %}<tr><td>{{ x.name }}</td><td>{{ x.phone }}</td><td>{{ x.points }}</td>
    <td>₹{{ "%.2f"|format(x.points*0.10) }}</td></tr>{% else %}<tr><td colspan="4">No customers.</td></tr>{% endfor %}
    </table></div></div>
    <div class="card"><h2>Loyalty Transaction History</h2><div class="tablewrap"><table>
    <tr><th>Bill</th><th>Date</th><th>Customer</th><th>Earned</th><th>Redeemed</th><th>Total</th></tr>
    {% for x in history %}<tr><td>{{ x.id }}</td><td>{{ x.created_at }}</td>
    <td>{{ x.customer_name }} {{ x.customer_phone or '' }}</td><td>{{ x.points_earned }}</td>
    <td>{{ x.points_redeemed }}</td><td>₹{{ "%.2f"|format(x.total) }}</td></tr>
    {% else %}<tr><td colspan="6">No transactions.</td></tr>{% endfor %}
    </table></div></div>
    """, customers_list=customers_list, balances=balances, totals=totals,
       current_points=current_points, history=history, start=start, end=end,
       customer_id=customer_id)


@app.route("/loyalty/export.csv")
@owner_required
def loyalty_export_csv():
    bid = session["business_id"]
    start = request.args.get("start_date", "").strip()
    end = request.args.get("end_date", "").strip()
    customer_id = request.args.get("customer_id", "").strip()

    customer_conditions = ["c.business_id=?"]
    params = [bid]
    sales_join = ["s.customer_id=c.id", "s.business_id=?"]
    sales_params = [bid]

    if customer_id:
        try:
            customer_conditions.append("c.id=?")
            params.append(int(customer_id))
        except ValueError:
            flash("Invalid customer filter.")
            return redirect(url_for("loyalty_accounts"))
    if start:
        sales_join.append("s.created_at>=?")
        sales_params.append(start + " 00:00:00")
    if end:
        sales_join.append("s.created_at<=?")
        sales_params.append(end + " 23:59:59")

    with connect() as c:
        rows = c.execute(f"""
            SELECT c.id,c.name,c.phone,c.points,
            COALESCE(SUM(s.points_earned),0) earned,
            COALESCE(SUM(s.points_redeemed),0) redeemed,
            COUNT(s.id) bills
            FROM customers c LEFT JOIN sales s ON {' AND '.join(sales_join)}
            WHERE {' AND '.join(customer_conditions)}
            GROUP BY c.id,c.name,c.phone,c.points ORDER BY c.name
        """, sales_params + params).fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "Customer ID", "Customer Name", "Phone", "Current Points",
        "Current Redemption Value INR", "Points Earned In Period",
        "Points Redeemed In Period", "Bills In Period"
    ])
    for r in rows:
        writer.writerow([
            r["id"], r["name"], r["phone"], r["points"],
            f"{r['points']*0.10:.2f}", r["earned"], r["redeemed"], r["bills"]
        ])
    response = make_response("\ufeff" + output.getvalue())
    response.headers["Content-Type"] = "text/csv; charset=utf-8"
    response.headers["Content-Disposition"] = 'attachment; filename="loyalty_report.csv"'
    return response


@app.route("/my-account")
@login_required
def customer_home():
    if current_role() != "customer":
        return redirect(url_for("home"))
    cid, bid = session["customer_id"], session["business_id"]
    with connect() as c:
        customer = c.execute("SELECT * FROM customers WHERE id=? AND business_id=?", (cid, bid)).fetchone()
        business = c.execute("SELECT * FROM businesses WHERE id=?", (bid,)).fetchone()
        sales = c.execute(
            "SELECT * FROM sales WHERE business_id=? AND customer_id=? ORDER BY id DESC",
            (bid, cid)
        ).fetchall()
    if not customer:
        session.clear()
        return redirect(url_for("login"))
    return page("My Account", """
    <div class="card"><h2>Welcome, {{ customer.name }}</h2><p>Business: {{ business.name }}</p>
    <p>Phone: {{ customer.phone }}</p><h2>Loyalty Points: {{ customer.points }}</h2>
    <p>Redemption value: ₹{{ "%.2f"|format(customer.points*0.10) }}</p></div>
    <div class="card"><h2>Purchase History</h2><div class="tablewrap"><table>
    <tr><th>Bill</th><th>Date</th><th>Total</th><th>Points Earned</th></tr>
    {% for s in sales %}<tr><td>{{ s.id }}</td><td>{{ s.created_at }}</td>
    <td>₹{{ "%.2f"|format(s.total) }}</td><td>{{ s.points_earned }}</td></tr>
    {% else %}<tr><td colspan="4">No purchases yet.</td></tr>{% endfor %}
    </table></div></div>
    """, customer=customer, business=business, sales=sales)


@app.errorhandler(500)
def server_error(error):
    app.logger.exception("Internal server error")
    return "Internal error. Check the application console for details.", 500


init_db()

if __name__ == "__main__":
    print("POS server starting on port 5000")
    print("Configure POS_SECRET_KEY, POS_SUPERADMIN_USERNAME, and POS_SUPERADMIN_PASSWORD in the environment.")
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)
