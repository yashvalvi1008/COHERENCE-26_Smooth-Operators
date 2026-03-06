"""
database.py - SQLite Database Initialization and Seeding for BudgetFlow Intelligence
Zero external dependencies. Uses Python standard library sqlite3 only.
"""

import sqlite3
import random
import math
import os

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "budgetflow.db")

DEPARTMENTS = [
    ("D001", "Ministry of Education", "Social"),
    ("D002", "Ministry of Health", "Social"),
    ("D003", "Ministry of Infrastructure", "Capital"),
    ("D004", "Ministry of Agriculture", "Rural"),
    ("D005", "Ministry of Defence", "Security"),
    ("D006", "Ministry of Finance", "Governance"),
    ("D007", "Ministry of Energy", "Capital"),
    ("D008", "Ministry of Housing", "Social"),
    ("D009", "Ministry of Transport", "Capital"),
    ("D010", "Ministry of Water Resources", "Rural"),
    ("D011", "Ministry of Science & Tech", "R&D"),
    ("D012", "Ministry of Commerce", "Economic"),
]

SCHEMES = [
    ("S001", "D001", "National Literacy Mission", 5000000000, "Education"),
    ("S002", "D001", "Mid-Day Meal Programme", 3500000000, "Education"),
    ("S003", "D002", "Ayushman Bharat Yojana", 8000000000, "Health"),
    ("S004", "D002", "National Health Mission", 6000000000, "Health"),
    ("S005", "D003", "Smart Cities Mission", 12000000000, "Infrastructure"),
    ("S006", "D003", "Highway Development Programme", 15000000000, "Infrastructure"),
    ("S007", "D004", "PM-KISAN Scheme", 7500000000, "Agriculture"),
    ("S008", "D004", "Crop Insurance Scheme", 4000000000, "Agriculture"),
    ("S009", "D005", "Defence Modernisation", 20000000000, "Security"),
    ("S010", "D007", "Solar Energy Mission", 9000000000, "Energy"),
    ("S011", "D008", "PM Awas Yojana", 11000000000, "Housing"),
    ("S012", "D009", "Metro Rail Projects", 13000000000, "Transport"),
    ("S013", "D010", "Jal Jeevan Mission", 6500000000, "Water"),
    ("S014", "D011", "Digital India Programme", 4500000000, "Technology"),
    ("S015", "D012", "Make in India Initiative", 5500000000, "Commerce"),
]

DISTRICTS = [
    ("DT001", "Mumbai", "Maharashtra", "West"),
    ("DT002", "Pune", "Maharashtra", "West"),
    ("DT003", "Delhi", "Delhi", "North"),
    ("DT004", "Gurugram", "Haryana", "North"),
    ("DT005", "Bengaluru", "Karnataka", "South"),
    ("DT006", "Chennai", "Tamil Nadu", "South"),
    ("DT007", "Hyderabad", "Telangana", "South"),
    ("DT008", "Kolkata", "West Bengal", "East"),
    ("DT009", "Patna", "Bihar", "East"),
    ("DT010", "Lucknow", "Uttar Pradesh", "North"),
    ("DT011", "Jaipur", "Rajasthan", "North"),
    ("DT012", "Bhopal", "Madhya Pradesh", "Central"),
    ("DT013", "Ahmedabad", "Gujarat", "West"),
    ("DT014", "Surat", "Gujarat", "West"),
    ("DT015", "Chandigarh", "Punjab", "North"),
    ("DT016", "Guwahati", "Assam", "East"),
    ("DT017", "Thiruvananthapuram", "Kerala", "South"),
    ("DT018", "Bhubaneswar", "Odisha", "East"),
    ("DT019", "Ranchi", "Jharkhand", "East"),
    ("DT020", "Raipur", "Chhattisgarh", "Central"),
]

VENDORS = [
    "Apex Constructions Ltd", "TechBridge Solutions", "National Supply Corp",
    "Bharat Materials Inc", "Indo Infrastructure Pvt Ltd", "Prime Contracts",
    "Galaxy Engineering Works", "Pioneer Builders", "United Services Group",
    "Alpha Procurement Ltd", "Horizon Projects", "Delta Contractors",
    "Sigma Suppliers", "Omega Build Corp", "Zenith Solutions Pvt Ltd",
]


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Create all tables and seed with realistic data."""
    conn = get_connection()
    cur = conn.cursor()

    # --- Schema ---
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS departments (
        id TEXT PRIMARY KEY, name TEXT, category TEXT,
        total_allocation REAL, utilized REAL, status TEXT
    );
    CREATE TABLE IF NOT EXISTS schemes (
        id TEXT PRIMARY KEY, department_id TEXT, name TEXT,
        allocation REAL, utilized REAL, category TEXT,
        start_date TEXT, end_date TEXT,
        FOREIGN KEY(department_id) REFERENCES departments(id)
    );
    CREATE TABLE IF NOT EXISTS districts (
        id TEXT PRIMARY KEY, name TEXT, state TEXT, region TEXT,
        allocation REAL, utilized REAL, performance_score REAL
    );
    CREATE TABLE IF NOT EXISTS transactions (
        id TEXT PRIMARY KEY, scheme_id TEXT, district_id TEXT,
        amount REAL, transaction_date TEXT, vendor TEXT,
        description TEXT, quarter INTEGER, year INTEGER,
        flagged INTEGER DEFAULT 0, flag_reason TEXT
    );
    CREATE TABLE IF NOT EXISTS anomalies (
        id TEXT PRIMARY KEY, transaction_id TEXT, department_id TEXT,
        type TEXT, severity TEXT, score REAL, description TEXT,
        detected_date TEXT, status TEXT DEFAULT 'Open'
    );
    CREATE TABLE IF NOT EXISTS allocations (
        id TEXT PRIMARY KEY, from_dept TEXT, to_dept TEXT,
        amount REAL, reason TEXT, status TEXT, created_date TEXT
    );
    CREATE TABLE IF NOT EXISTS risk_scores (
        id TEXT PRIMARY KEY, department_id TEXT, score REAL,
        lapse_probability REAL, forecast_q4 REAL, assessment_date TEXT
    );
    """)

    cur.execute("SELECT COUNT(*) FROM departments")
    if cur.fetchone()[0] > 0:
        conn.close()
        return

    random.seed(42)

    # --- Seed Departments ---
    dept_rows = []
    for dept_id, name, category in DEPARTMENTS:
        total_alloc = random.uniform(3e9, 20e9)
        util_pct = random.uniform(0.38, 0.91)
        utilized = total_alloc * util_pct
        status = "Critical" if util_pct < 0.5 else ("Warning" if util_pct < 0.7 else "On Track")
        dept_rows.append((dept_id, name, category, total_alloc, utilized, status))

    cur.executemany(
        "INSERT INTO departments VALUES (?,?,?,?,?,?)", dept_rows
    )

    # --- Seed Schemes ---
    scheme_rows = []
    for scheme_id, dept_id, name, alloc, category in SCHEMES:
        util_pct = random.uniform(0.35, 0.95)
        utilized = alloc * util_pct
        start_date = "2025-04-01"
        end_date = "2026-03-31"
        scheme_rows.append((scheme_id, dept_id, name, alloc, utilized, category, start_date, end_date))

    cur.executemany(
        "INSERT INTO schemes VALUES (?,?,?,?,?,?,?,?)", scheme_rows
    )

    # --- Seed Districts ---
    district_rows = []
    for dist_id, name, state, region in DISTRICTS:
        alloc = random.uniform(5e8, 3e9)
        util_pct = random.uniform(0.40, 0.95)
        district_rows.append((
            dist_id, name, state, region, alloc, alloc * util_pct,
            round(util_pct * 100, 2)
        ))
    cur.executemany("INSERT INTO districts VALUES (?,?,?,?,?,?,?)", district_rows)

    # --- Seed Transactions (~800) ---
    transactions = []
    anomalies = []
    anom_counter = 1
    tx_counter = 1
    quarters_months = {1: [4, 5, 6], 2: [7, 8, 9], 3: [10, 11, 12], 4: [1, 2, 3]}

    for i in range(800):
        tx_id = f"TX{tx_counter:04d}"
        tx_counter += 1
        scheme = random.choice(SCHEMES)
        scheme_id = scheme[0]
        dept_id = scheme[1]
        district = random.choice(DISTRICTS)
        dist_id = district[0]
        year = 2025
        quarter = random.randint(1, 4)
        if quarter == 4:
            year = 2026
        month = random.choice(quarters_months[quarter])
        day = random.randint(1, 28)
        tx_date = f"{year}-{month:02d}-{day:02d}"
        vendor = random.choice(VENDORS)
        base_amount = random.uniform(1e6, 5e7)
        flagged = 0
        flag_reason = ""
        description = f"Payment for {scheme[2]} implementation"

        # Inject anomalies into ~12% of transactions
        anomaly_type = None
        severity = None
        anom_score = 0

        r = random.random()
        if r < 0.04:
            # Year-end rushing: Q4 with very large amount
            quarter = 4
            year = 2026
            month = random.choice([1, 2, 3])
            tx_date = f"{year}-{month:02d}-{day:02d}"
            base_amount = random.uniform(4e7, 1.5e8)
            flagged = 1
            flag_reason = "Year-End Rushing"
            anomaly_type = "Year-End Rushing"
            severity = "High"
            anom_score = random.uniform(70, 90)
        elif r < 0.07:
            # Salami slicing: small amounts just below threshold
            base_amount = random.uniform(990000, 999999)
            flagged = 1
            flag_reason = "Salami Slicing"
            anomaly_type = "Salami Slicing"
            severity = "Critical"
            anom_score = random.uniform(80, 98)
        elif r < 0.10:
            # Statistical outlier: massive spike
            base_amount = random.uniform(2e8, 8e8)
            flagged = 1
            flag_reason = "Statistical Outlier (Z-Score)"
            anomaly_type = "Statistical Outlier"
            severity = "Critical"
            anom_score = random.uniform(85, 99)
        elif r < 0.12:
            # Benford's Law violation
            base_amount = random.uniform(6e6, 9e7)
            flagged = 1
            flag_reason = "Benford's Law Violation"
            anomaly_type = "Benford Violation"
            severity = "Medium"
            anom_score = random.uniform(55, 75)

        transactions.append((
            tx_id, scheme_id, dist_id, round(base_amount, 2), tx_date,
            vendor, description, quarter, year, flagged, flag_reason
        ))

        if flagged:
            anom_id = f"AN{anom_counter:04d}"
            anom_counter += 1
            anomalies.append((
                anom_id, tx_id, dept_id, anomaly_type, severity,
                round(anom_score, 2), f"{anomaly_type} detected in {vendor} transaction of ₹{base_amount/1e6:.1f}M",
                tx_date, "Open"
            ))

    cur.executemany(
        "INSERT INTO transactions VALUES (?,?,?,?,?,?,?,?,?,?,?)", transactions
    )
    cur.executemany(
        "INSERT INTO anomalies VALUES (?,?,?,?,?,?,?,?,?)", anomalies
    )

    # --- Seed Risk Scores ---
    risk_rows = []
    for dept_id, name, category in DEPARTMENTS:
        score = random.uniform(20, 95)
        lapse_prob = round(max(0, min(1, (100 - score) / 100 * random.uniform(0.8, 1.2))), 3)
        forecast_q4 = random.uniform(0.3, 0.9)
        risk_rows.append((
            f"RS{DEPARTMENTS.index((dept_id, name, category)) + 1:03d}",
            dept_id, round(score, 2), lapse_prob, round(forecast_q4, 3),
            "2026-03-07"
        ))
    cur.executemany("INSERT INTO risk_scores VALUES (?,?,?,?,?,?)", risk_rows)

    conn.commit()
    conn.close()
    print(f"[DB] Initialized with {len(transactions)} transactions, {len(anomalies)} anomalies.")


if __name__ == "__main__":
    init_db()
    print(f"[DB] Database ready at: {DB_PATH}")
