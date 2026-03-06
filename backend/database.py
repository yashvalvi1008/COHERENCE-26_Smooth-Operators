import sqlite3
import random
import statistics
from datetime import datetime, timedelta
import os

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "budgetflow.db")

random.seed(42)


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


SCHEMA = """
CREATE TABLE IF NOT EXISTS fiscal_years (
    id TEXT PRIMARY KEY,
    year_label TEXT NOT NULL,
    start_date TEXT NOT NULL,
    end_date TEXT NOT NULL,
    is_current INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS departments (
    id TEXT PRIMARY KEY,
    code TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    short_name TEXT NOT NULL,
    category TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS schemes (
    id TEXT PRIMARY KEY,
    code TEXT UNIQUE NOT NULL,
    dept_id TEXT NOT NULL,
    name TEXT NOT NULL,
    total_allocation REAL NOT NULL,
    category TEXT NOT NULL,
    FOREIGN KEY (dept_id) REFERENCES departments(id)
);

CREATE TABLE IF NOT EXISTS districts (
    id TEXT PRIMARY KEY,
    code TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    state TEXT NOT NULL,
    district_type TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS allocations (
    id TEXT PRIMARY KEY,
    scheme_id TEXT NOT NULL,
    district_id TEXT NOT NULL,
    fiscal_year_id TEXT NOT NULL,
    allocated_amount REAL NOT NULL,
    utilized_amount REAL NOT NULL,
    quarter INTEGER NOT NULL,
    FOREIGN KEY (scheme_id) REFERENCES schemes(id),
    FOREIGN KEY (district_id) REFERENCES districts(id),
    FOREIGN KEY (fiscal_year_id) REFERENCES fiscal_years(id)
);

CREATE TABLE IF NOT EXISTS transactions (
    id TEXT PRIMARY KEY,
    scheme_id TEXT NOT NULL,
    district_id TEXT NOT NULL,
    fiscal_year_id TEXT NOT NULL,
    amount REAL NOT NULL,
    txn_date TEXT NOT NULL,
    txn_type TEXT NOT NULL,
    vendor TEXT,
    description TEXT,
    FOREIGN KEY (scheme_id) REFERENCES schemes(id),
    FOREIGN KEY (district_id) REFERENCES districts(id),
    FOREIGN KEY (fiscal_year_id) REFERENCES fiscal_years(id)
);

CREATE TABLE IF NOT EXISTS anomalies (
    id TEXT PRIMARY KEY,
    txn_id TEXT,
    scheme_id TEXT NOT NULL,
    dept_id TEXT NOT NULL,
    anomaly_type TEXT NOT NULL,
    severity TEXT NOT NULL,
    score REAL NOT NULL,
    description TEXT NOT NULL,
    status TEXT DEFAULT 'open',
    detected_at TEXT NOT NULL,
    FOREIGN KEY (scheme_id) REFERENCES schemes(id),
    FOREIGN KEY (dept_id) REFERENCES departments(id)
);

CREATE TABLE IF NOT EXISTS risk_scores (
    id TEXT PRIMARY KEY,
    dept_id TEXT NOT NULL,
    scheme_id TEXT NOT NULL,
    fiscal_year_id TEXT NOT NULL,
    composite_score REAL NOT NULL,
    zscore_flag INTEGER DEFAULT 0,
    benford_flag INTEGER DEFAULT 0,
    salami_flag INTEGER DEFAULT 0,
    yearend_flag INTEGER DEFAULT 0,
    lapse_probability REAL DEFAULT 0,
    calculated_at TEXT NOT NULL,
    FOREIGN KEY (dept_id) REFERENCES departments(id),
    FOREIGN KEY (scheme_id) REFERENCES schemes(id)
);

CREATE TABLE IF NOT EXISTS reallocation_recommendations (
    id TEXT PRIMARY KEY,
    from_scheme_id TEXT NOT NULL,
    to_scheme_id TEXT NOT NULL,
    from_dept_id TEXT NOT NULL,
    to_dept_id TEXT NOT NULL,
    amount REAL NOT NULL,
    reason TEXT NOT NULL,
    status TEXT DEFAULT 'pending',
    created_at TEXT NOT NULL,
    FOREIGN KEY (from_scheme_id) REFERENCES schemes(id),
    FOREIGN KEY (to_scheme_id) REFERENCES schemes(id)
);

CREATE TABLE IF NOT EXISTS audit_log (
    id TEXT PRIMARY KEY,
    action_type TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    description TEXT NOT NULL,
    performed_by TEXT DEFAULT 'system',
    performed_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS benchmarks (
    id TEXT PRIMARY KEY,
    district_id TEXT NOT NULL,
    state TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    metric_value REAL NOT NULL,
    benchmark_value REAL NOT NULL,
    fiscal_year_id TEXT NOT NULL,
    FOREIGN KEY (district_id) REFERENCES districts(id),
    FOREIGN KEY (fiscal_year_id) REFERENCES fiscal_years(id)
);
"""

FISCAL_YEARS_DATA = [
    ("FY2023", "2022-23", "2022-04-01", "2023-03-31", 0),
    ("FY2024", "2023-24", "2023-04-01", "2024-03-31", 0),
    ("FY2025", "2024-25", "2024-04-01", "2025-03-31", 1),
]

DEPARTMENTS_DATA = [
    ("D001", "DEPT001", "Health & Family Welfare", "HFW", "healthcare"),
    ("D002", "DEPT002", "Education", "EDU", "education"),
    ("D003", "DEPT003", "Rural Development", "RD", "rural"),
    ("D004", "DEPT004", "Agriculture", "AGR", "agriculture"),
    ("D005", "DEPT005", "Infrastructure & Transport", "INF", "infrastructure"),
    ("D006", "DEPT006", "Water Resources", "WR", "water"),
    ("D007", "DEPT007", "Social Justice", "SJ", "social"),
    ("D008", "DEPT008", "Science & Technology", "ST", "technology"),
    ("D009", "DEPT009", "Urban Development", "UD", "urban"),
    ("D010", "DEPT010", "Finance & Planning", "FIN", "finance"),
]

# Allocations in Crore INR
SCHEMES_DATA = [
    ("S001", "SCH001", "D001", "National Health Mission",       37000, "healthcare"),
    ("S002", "SCH002", "D001", "Ayushman Bharat",               7200,  "healthcare"),
    ("S003", "SCH003", "D002", "Sarva Shiksha Abhiyan",         33000, "education"),
    ("S004", "SCH004", "D002", "Mid-Day Meal Scheme",           13000, "education"),
    ("S005", "SCH005", "D003", "MGNREGA",                       60000, "rural"),
    ("S006", "SCH006", "D003", "PMGSY - Rural Roads",           19000, "rural"),
    ("S007", "SCH007", "D004", "PM Kisan Samman Nidhi",         60000, "agriculture"),
    ("S008", "SCH008", "D004", "Fasal Bima Yojana",             15500, "agriculture"),
    ("S009", "SCH009", "D005", "Bharatmala Pariyojana",         97636, "infrastructure"),
    ("S010", "SCH010", "D005", "Smart Cities Mission",          48000, "infrastructure"),
    ("S011", "SCH011", "D006", "Jal Jeevan Mission",            60000, "water"),
    ("S012", "SCH012", "D006", "PMKSY Water Reform",            50000, "water"),
    ("S013", "SCH013", "D007", "PM Awas Yojana Urban",          77640, "social"),
    ("S014", "SCH014", "D007", "Ujjwala Yojana",                8000,  "social"),
    ("S015", "SCH015", "D008", "Digital India",                 11000, "technology"),
]

DISTRICTS_DATA = [
    ("DIS001", "DIST001", "Mumbai",     "Maharashtra",   "urban"),
    ("DIS002", "DIST002", "Pune",       "Maharashtra",   "urban"),
    ("DIS003", "DIST003", "Nashik",     "Maharashtra",   "semi-urban"),
    ("DIS004", "DIST004", "Delhi",      "Delhi",         "urban"),
    ("DIS005", "DIST005", "Gurugram",   "Haryana",       "urban"),
    ("DIS006", "DIST006", "Faridabad",  "Haryana",       "semi-urban"),
    ("DIS007", "DIST007", "Bengaluru",  "Karnataka",     "urban"),
    ("DIS008", "DIST008", "Mysuru",     "Karnataka",     "semi-urban"),
    ("DIS009", "DIST009", "Chennai",    "Tamil Nadu",    "urban"),
    ("DIS010", "DIST010", "Coimbatore", "Tamil Nadu",    "semi-urban"),
    ("DIS011", "DIST011", "Hyderabad",  "Telangana",     "urban"),
    ("DIS012", "DIST012", "Warangal",   "Telangana",     "semi-urban"),
    ("DIS013", "DIST013", "Kolkata",    "West Bengal",   "urban"),
    ("DIS014", "DIST014", "Howrah",     "West Bengal",   "urban"),
    ("DIS015", "DIST015", "Patna",      "Bihar",         "urban"),
    ("DIS016", "DIST016", "Gaya",       "Bihar",         "rural"),
    ("DIS017", "DIST017", "Lucknow",    "Uttar Pradesh", "urban"),
    ("DIS018", "DIST018", "Kanpur",     "Uttar Pradesh", "urban"),
    ("DIS019", "DIST019", "Jaipur",     "Rajasthan",     "urban"),
    ("DIS020", "DIST020", "Udaipur",    "Rajasthan",     "semi-urban"),
]

VENDORS = [
    "Infra Constructions Ltd", "MedSupply Corp", "EduTech Solutions",
    "AgroServe Pvt Ltd", "SmartCity Technologies", "WaterWorks India",
    "RuralBuild Contractors", "HealthFirst Pvt Ltd", "Digital Solutions Co",
    "National Services Corp", "State Procurement Office", "District Agency A",
    "District Agency B", "Central Implementation Unit", "NGO Partner Trust",
    "L&T Infrastructure", "RITES Ltd", "NBCC India", "WAPCOS Ltd",
]

TXN_TYPES = ["disbursement", "procurement", "transfer", "grant", "reimbursement"]


def _get_fiscal_quarter(month, fiscal_start_month=4):
    offset = (month - fiscal_start_month) % 12
    return (offset // 3) + 1


def generate_transactions(conn):
    schemes = conn.execute("SELECT id, total_allocation, dept_id FROM schemes").fetchall()
    districts = conn.execute("SELECT id FROM districts").fetchall()
    district_ids = [d["id"] for d in districts]
    fy_map = {"FY2023": 2022, "FY2024": 2023, "FY2025": 2024}

    transactions = []
    txn_count = 0

    for fy_id, base_year in fy_map.items():
        fy_start = datetime(base_year, 4, 1)

        for scheme in schemes:
            scheme_id = scheme["id"]
            allocation = scheme["total_allocation"]
            num_txns = random.randint(14, 22)

            for i in range(num_txns):
                dist_id = random.choice(district_ids)
                # Q4 gets slightly more transactions
                if i < num_txns * 0.65:
                    quarter = random.randint(1, 3)
                    month_offset = (quarter - 1) * 3 + random.randint(0, 2)
                else:
                    quarter = 4
                    month_offset = 9 + random.randint(0, 2)

                day_offset = month_offset * 30 + random.randint(1, 27)
                txn_date = fy_start + timedelta(days=day_offset)

                base_amount = (allocation / num_txns) * random.uniform(0.4, 1.8)
                txn_id = f"TXN{txn_count:05d}"
                vendor = random.choice(VENDORS)
                txn_type = random.choice(TXN_TYPES)

                transactions.append((
                    txn_id, scheme_id, dist_id, fy_id,
                    round(base_amount, 2),
                    txn_date.strftime("%Y-%m-%d"),
                    txn_type, vendor,
                    f"Payment ref {scheme_id} Q{quarter} batch",
                ))
                txn_count += 1

    # --- Inject anomalous transactions (FY2025 only) ---
    schemes_list = [dict(s) for s in schemes]

    # 1. Z-score spikes (5 massive outliers)
    for i in range(5):
        s = random.choice(schemes_list)
        dist_id = random.choice(district_ids)
        amount = s["total_allocation"] * random.uniform(0.9, 1.4)
        txn_date = datetime(2025, random.randint(1, 3), random.randint(10, 28))
        transactions.append((
            f"TXNANOM{i:03d}", s["id"], dist_id, "FY2025",
            round(amount, 2), txn_date.strftime("%Y-%m-%d"),
            "disbursement", "Suspicious Vendor Corp",
            "Anomalous large-value disbursement",
        ))
        txn_count += 1

    # 2. Salami slicing (8 clustered below threshold on same vendor)
    salami_scheme = schemes_list[0]
    threshold = salami_scheme["total_allocation"] * 0.0012
    for i in range(8):
        dist_id = random.choice(district_ids)
        amount = threshold * random.uniform(0.84, 0.99)
        txn_date = datetime(2025, 2, random.randint(1, 27))
        transactions.append((
            f"TXNSAL{i:03d}", salami_scheme["id"], dist_id, "FY2025",
            round(amount, 2), txn_date.strftime("%Y-%m-%d"),
            "procurement", "Shell Holdings XYZ",
            f"Procurement batch#{i+1} below approval limit",
        ))
        txn_count += 1

    # 3. Year-end rush (6 large Q4 payments for MGNREGA)
    ye_scheme = schemes_list[4]  # MGNREGA
    for i in range(6):
        dist_id = random.choice(district_ids)
        amount = ye_scheme["total_allocation"] * random.uniform(0.07, 0.10)
        txn_date = datetime(2025, 3, random.randint(1, 30))
        transactions.append((
            f"TXNYER{i:03d}", ye_scheme["id"], dist_id, "FY2025",
            round(amount, 2), txn_date.strftime("%Y-%m-%d"),
            "disbursement", "FastTrack Contractors",
            f"Year-end accelerated payment #{i+1}",
        ))
        txn_count += 1

    return transactions


def generate_allocations(conn):
    schemes = conn.execute("SELECT id, total_allocation FROM schemes").fetchall()
    districts = conn.execute("SELECT id FROM districts").fetchall()
    district_ids = [d["id"] for d in districts]
    fy_ids = ["FY2023", "FY2024", "FY2025"]

    allocations = []
    alloc_count = 0

    for fy_id in fy_ids:
        for scheme in schemes:
            for quarter in range(1, 5):
                # Allocate to 5 random districts each quarter
                sample_districts = random.sample(district_ids, 5)
                for dist_id in sample_districts:
                    per_unit = scheme["total_allocation"] / 20.0
                    allocated = per_unit * random.uniform(0.85, 1.15)
                    util_rate = random.uniform(0.42, 0.94)
                    if quarter == 4:
                        util_rate = random.uniform(0.60, 0.99)
                    utilized = allocated * util_rate

                    allocations.append((
                        f"ALLOC{alloc_count:05d}",
                        scheme["id"], dist_id, fy_id,
                        round(allocated, 2), round(utilized, 2), quarter,
                    ))
                    alloc_count += 1

    return allocations


def generate_benchmarks(conn):
    districts = conn.execute("SELECT id, state FROM districts").fetchall()
    fy_ids = ["FY2023", "FY2024", "FY2025"]
    metrics = [
        ("utilization_rate",          0.75, 0.10),
        ("disbursement_efficiency",   0.80, 0.12),
        ("procurement_compliance",    0.85, 0.08),
        ("outcome_score",             0.70, 0.15),
    ]

    benchmarks = []
    bmark_count = 0
    for fy_id in fy_ids:
        for dist in districts:
            for metric_name, benchmark_val, std in metrics:
                value = max(0.10, min(1.0, random.gauss(benchmark_val, std)))
                benchmarks.append((
                    f"BMARK{bmark_count:05d}",
                    dist["id"], dist["state"],
                    metric_name, round(value, 4), benchmark_val, fy_id,
                ))
                bmark_count += 1

    return benchmarks


def initialize_db(force=False):
    if os.path.exists(DB_PATH) and not force:
        return

    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    conn = get_connection()
    conn.executescript(SCHEMA)

    conn.executemany("INSERT INTO fiscal_years VALUES (?,?,?,?,?)", FISCAL_YEARS_DATA)
    conn.executemany("INSERT INTO departments VALUES (?,?,?,?,?)", DEPARTMENTS_DATA)
    conn.executemany("INSERT INTO schemes VALUES (?,?,?,?,?,?)", SCHEMES_DATA)
    conn.executemany("INSERT INTO districts VALUES (?,?,?,?,?)", DISTRICTS_DATA)

    allocs = generate_allocations(conn)
    conn.executemany("INSERT INTO allocations VALUES (?,?,?,?,?,?,?)", allocs)

    txns = generate_transactions(conn)
    conn.executemany("INSERT INTO transactions VALUES (?,?,?,?,?,?,?,?,?)", txns)

    bmarks = generate_benchmarks(conn)
    conn.executemany("INSERT INTO benchmarks VALUES (?,?,?,?,?,?,?)", bmarks)

    now = datetime.now().isoformat()
    conn.executemany("INSERT INTO audit_log VALUES (?,?,?,?,?,?,?)", [
        ("AUD00000", "system_init", "database", "DB001",
         "Database initialized with seed data", "system", now),
        ("AUD00001", "data_load", "transactions", "BATCH001",
         f"Loaded {len(txns)} transaction records", "system", now),
        ("AUD00002", "data_load", "allocations", "BATCH002",
         f"Loaded {len(allocs)} allocation records", "system", now),
    ])

    conn.commit()
    conn.close()
    print(f"[DB] Initialized at {DB_PATH}")
    print(f"[DB] {len(allocs)} allocations | {len(txns)} transactions | {len(bmarks)} benchmarks")


if __name__ == "__main__":
    initialize_db(force=True)
