"""
analytics.py - Pure Python Intelligence Engine for BudgetFlow Intelligence
Zero external dependencies. Uses Python standard library math only.
Implements: Z-Score, IQR, Benford's Law, OLS Regression, Exponential Smoothing, Greedy Optimizer.
"""

import math
import sqlite3
import json
from database import get_connection, DB_PATH


# ─────────────────────────────────────────────
# I. CORE MATH UTILITIES
# ─────────────────────────────────────────────

def mean(data):
    return sum(data) / len(data) if data else 0.0


def variance(data):
    if len(data) < 2:
        return 0.0
    m = mean(data)
    return sum((x - m) ** 2 for x in data) / (len(data) - 1)


def std_dev(data):
    return math.sqrt(variance(data))


def z_scores(data):
    mu = mean(data)
    sigma = std_dev(data)
    if sigma == 0:
        return [0.0] * len(data)
    return [(x - mu) / sigma for x in data]


def iqr_bounds(data):
    sorted_data = sorted(data)
    n = len(sorted_data)
    q1 = sorted_data[n // 4]
    q3 = sorted_data[(3 * n) // 4]
    iqr = q3 - q1
    return q1 - 1.5 * iqr, q3 + 1.5 * iqr


def ols_regression(x_vals, y_vals):
    """Ordinary Least Squares: returns (slope, intercept)."""
    n = len(x_vals)
    if n < 2:
        return 0.0, y_vals[0] if y_vals else 0.0
    sum_x = sum(x_vals)
    sum_y = sum(y_vals)
    sum_xy = sum(x * y for x, y in zip(x_vals, y_vals))
    sum_xx = sum(x * x for x in x_vals)
    denom = n * sum_xx - sum_x ** 2
    if denom == 0:
        return 0.0, mean(y_vals)
    slope = (n * sum_xy - sum_x * sum_y) / denom
    intercept = (sum_y - slope * sum_x) / n
    return slope, intercept


def exponential_smoothing(data, alpha=0.35):
    """Exponential smoothing forecasting."""
    if not data:
        return []
    smoothed = [data[0]]
    for i in range(1, len(data)):
        smoothed.append(alpha * data[i] + (1 - alpha) * smoothed[-1])
    return smoothed


def benford_expected():
    """Benford's Law: expected first-digit distribution."""
    return {d: math.log10(1 + 1 / d) for d in range(1, 10)}


def benford_chi_square(amounts):
    """Compute chi-square statistic vs Benford's Law for a set of amounts."""
    counts = {d: 0 for d in range(1, 10)}
    total = 0
    for amt in amounts:
        first_digit = int(str(abs(int(amt)))[0]) if amt > 0 else 0
        if first_digit in counts:
            counts[first_digit] += 1
            total += 1
    if total == 0:
        return 0.0, {}
    observed_freq = {d: counts[d] / total for d in range(1, 10)}
    expected = benford_expected()
    chi_sq = sum(
        ((observed_freq[d] - expected[d]) ** 2) / expected[d]
        for d in range(1, 10)
    )
    return round(chi_sq, 4), {
        "observed": {str(k): round(v, 4) for k, v in observed_freq.items()},
        "expected": {str(k): round(v, 4) for k, v in expected.items()},
    }


# ─────────────────────────────────────────────
# II. ANOMALY DETECTION ENGINE
# ─────────────────────────────────────────────

def composite_risk_score(z, is_benford_violation, is_salami, is_year_end_rush, iqr_outlier):
    """Weighted composite risk score (0-100)."""
    score = 0
    if abs(z) > 3:
        score += 40
    elif abs(z) > 2:
        score += 25
    if is_benford_violation:
        score += 20
    if is_salami:
        score += 30
    if is_year_end_rush:
        score += 15
    if iqr_outlier:
        score += 15
    return min(round(score, 2), 100)


def run_anomaly_scan():
    """Full anomaly detection scan over all transactions."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM transactions ORDER BY transaction_date")
    txs = [dict(row) for row in cur.fetchall()]
    amounts = [tx["amount"] for tx in txs]

    zs = z_scores(amounts)
    low_iqr, high_iqr = iqr_bounds(amounts)
    benford_chi, benford_dist = benford_chi_square(amounts)

    # Q4 spending analysis
    q4_amounts = [tx["amount"] for tx in txs if tx["quarter"] == 4]
    total_amount = sum(amounts) if amounts else 1
    q4_total = sum(q4_amounts)
    q4_pct = (q4_total / total_amount * 100) if total_amount > 0 else 0

    # Salami slicing: multiple transactions to same vendor just below 1M
    vendor_small_txs = {}
    for tx in txs:
        if 900000 <= tx["amount"] <= 1000000:
            vendor_small_txs[tx["vendor"]] = vendor_small_txs.get(tx["vendor"], 0) + 1

    salami_vendors = {v: c for v, c in vendor_small_txs.items() if c >= 3}

    results = {
        "total_transactions": len(txs),
        "flagged_count": sum(1 for tx in txs if tx["flagged"]),
        "benford_chi_square": benford_chi,
        "benford_distribution": benford_dist,
        "q4_spending_pct": round(q4_pct, 2),
        "q4_rushing_flag": q4_pct > 45,
        "salami_slicing_vendors": salami_vendors,
        "outlier_count": sum(1 for z in zs if abs(z) > 2.5),
    }
    conn.close()
    return results


def get_anomaly_list(limit=50, severity=None):
    """Retrieve anomalies from the database."""
    conn = get_connection()
    cur = conn.cursor()
    query = """
        SELECT a.*, d.name as dept_name
        FROM anomalies a
        LEFT JOIN departments d ON a.department_id = d.id
    """
    params = []
    if severity:
        query += " WHERE a.severity = ?"
        params.append(severity)
    query += " ORDER BY a.score DESC LIMIT ?"
    params.append(limit)
    cur.execute(query, params)
    rows = [dict(row) for row in cur.fetchall()]
    conn.close()
    return rows


# ─────────────────────────────────────────────
# III. PREDICTIVE RISK ENGINE
# ─────────────────────────────────────────────

def predict_lapse_risk():
    """Predict lapse probability for each department using OLS + Exponential Smoothing."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT t.scheme_id, s.department_id, t.quarter, SUM(t.amount) as total
        FROM transactions t
        JOIN schemes s ON t.scheme_id = s.id
        GROUP BY s.department_id, t.quarter
        ORDER BY s.department_id, t.quarter
    """)
    rows = cur.fetchall()

    dept_quarterly = {}
    for row in rows:
        dept_id = row["department_id"]
        q = row["quarter"]
        total = row["total"]
        if dept_id not in dept_quarterly:
            dept_quarterly[dept_id] = {1: 0, 2: 0, 3: 0, 4: 0}
        dept_quarterly[dept_id][q] = total

    cur.execute("SELECT * FROM departments")
    depts = {row["id"]: dict(row) for row in cur.fetchall()}
    cur.execute("SELECT * FROM risk_scores")
    risk_scores_db = {row["department_id"]: dict(row) for row in cur.fetchall()}
    conn.close()

    predictions = []
    for dept_id, quarterly in dept_quarterly.items():
        if dept_id not in depts:
            continue
        dept = depts[dept_id]
        q_vals = [quarterly.get(q, 0) for q in [1, 2, 3]]
        smoothed = exponential_smoothing(q_vals, alpha=0.35)
        slope, intercept = ols_regression([1, 2, 3], q_vals)
        q4_forecast = slope * 4 + intercept
        if q4_forecast < 0:
            q4_forecast = smoothed[-1] if smoothed else 0

        total_alloc = dept["total_allocation"]
        total_spent = dept["utilized"]
        remaining = total_alloc - total_spent
        lapse_probability = max(0, min(1, (remaining - q4_forecast) / total_alloc)) if total_alloc > 0 else 0
        lapse_probability = round(lapse_probability, 3)

        rs = risk_scores_db.get(dept_id, {})
        predictions.append({
            "department_id": dept_id,
            "department_name": dept["name"],
            "category": dept["category"],
            "total_allocation": total_alloc,
            "utilized": total_spent,
            "utilization_pct": round(total_spent / total_alloc * 100, 1) if total_alloc else 0,
            "q4_forecast": round(q4_forecast, 2),
            "lapse_probability": lapse_probability,
            "risk_score": rs.get("score", 50),
            "status": dept["status"],
            "quarterly_spend": quarterly,
        })

    predictions.sort(key=lambda x: x["lapse_probability"], reverse=True)
    return predictions


# ─────────────────────────────────────────────
# IV. REALLOCATION OPTIMIZER
# ─────────────────────────────────────────────

def optimize_reallocations():
    """Greedy algorithm to pair surplus departments with deficit departments."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM departments")
    depts = [dict(row) for row in cur.fetchall()]
    conn.close()

    surplus = []
    deficit = []
    for d in depts:
        remaining = d["total_allocation"] - d["utilized"]
        util_pct = d["utilized"] / d["total_allocation"] if d["total_allocation"] else 0
        if util_pct < 0.55 and remaining > 1e8:
            surplus.append({**d, "surplus_amount": remaining * 0.4, "util_pct": util_pct})
        elif util_pct > 0.82:
            need = d["total_allocation"] * 0.15
            deficit.append({**d, "deficit_amount": need, "util_pct": util_pct})

    surplus.sort(key=lambda x: x["surplus_amount"], reverse=True)
    deficit.sort(key=lambda x: x["deficit_amount"], reverse=True)

    recommendations = []
    used_surplus = {}
    for d in deficit:
        for s in surplus:
            available = s["surplus_amount"] - used_surplus.get(s["id"], 0)
            if available <= 0:
                continue
            transfer = min(available, d["deficit_amount"])
            if transfer < 1e7:
                continue
            used_surplus[s["id"]] = used_surplus.get(s["id"], 0) + transfer
            recommendations.append({
                "from_dept_id": s["id"],
                "from_dept_name": s["name"],
                "to_dept_id": d["id"],
                "to_dept_name": d["name"],
                "amount": round(transfer, 2),
                "from_utilization": round(s["util_pct"] * 100, 1),
                "to_utilization": round(d["util_pct"] * 100, 1),
                "reason": f"Rebalance: {s['name']} has {s['util_pct']*100:.0f}% utilization, {d['name']} needs ₹{transfer/1e7:.1f}Cr",
                "legal_compliance": "Match Purpose" if s["category"] == d["category"] else "Cross-Scheme Transfer",
                "status": "Recommended",
            })
    return recommendations


# ─────────────────────────────────────────────
# V. OVERVIEW AGGREGATION
# ─────────────────────────────────────────────

def get_overview():
    """Aggregate KPI statistics for the dashboard overview."""
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT SUM(total_allocation) as total_alloc, SUM(utilized) as total_util FROM departments")
    row = dict(cur.fetchone())
    total_alloc = row["total_alloc"] or 0
    total_util = row["total_util"] or 0

    cur.execute("SELECT COUNT(*) as cnt FROM anomalies WHERE status='Open'")
    open_alerts = cur.fetchone()["cnt"]

    cur.execute("SELECT COUNT(*) as cnt FROM anomalies WHERE severity='Critical' AND status='Open'")
    critical_alerts = cur.fetchone()["cnt"]

    cur.execute("SELECT COUNT(*) as cnt FROM transactions")
    tx_count = cur.fetchone()["cnt"]

    cur.execute("SELECT COUNT(*) as cnt FROM transactions WHERE flagged=1")
    flagged_count = cur.fetchone()["cnt"]

    cur.execute("SELECT * FROM departments ORDER BY utilized/total_allocation ASC LIMIT 3")
    low_util = [dict(r) for r in cur.fetchall()]

    cur.execute("SELECT COUNT(DISTINCT id) as cnt FROM schemes")
    scheme_count = cur.fetchone()["cnt"]

    cur.execute("SELECT * FROM anomalies WHERE status='Open' ORDER BY score DESC LIMIT 5")
    top_anomalies = [dict(r) for r in cur.fetchall()]

    # Monthly spending trend (last 12 months)
    cur.execute("""
        SELECT substr(transaction_date, 1, 7) as month, SUM(amount) as total
        FROM transactions
        GROUP BY month ORDER BY month DESC LIMIT 12
    """)
    monthly_trend = [dict(r) for r in cur.fetchall()]
    monthly_trend.reverse()

    cur.execute("""
        SELECT d.name, d.total_allocation, d.utilized,
               (d.utilized * 100.0 / d.total_allocation) as util_pct
        FROM departments d ORDER BY util_pct DESC
    """)
    dept_util = [dict(r) for r in cur.fetchall()]

    conn.close()

    return {
        "total_allocation": round(total_alloc, 2),
        "total_utilized": round(total_util, 2),
        "utilization_pct": round(total_util / total_alloc * 100, 2) if total_alloc else 0,
        "open_alerts": open_alerts,
        "critical_alerts": critical_alerts,
        "total_transactions": tx_count,
        "flagged_transactions": flagged_count,
        "active_schemes": scheme_count,
        "low_utilization_depts": low_util,
        "top_anomalies": top_anomalies,
        "monthly_trend": monthly_trend,
        "dept_utilization": dept_util,
    }


def get_fund_flow():
    """Get Sankey diagram data: Ministry -> Scheme -> District funds."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT d.name as dept_name, s.name as scheme_name, s.allocation, s.utilized, s.category
        FROM schemes s JOIN departments d ON s.department_id = d.id
        ORDER BY s.allocation DESC LIMIT 12
    """)
    scheme_rows = [dict(r) for r in cur.fetchall()]

    cur.execute("""
        SELECT dt.name as dist_name, dt.region, dt.allocation AS dist_alloc, dt.utilized AS dist_util
        FROM districts dt ORDER BY dt.utilized DESC LIMIT 10
    """)
    dist_rows = [dict(r) for r in cur.fetchall()]

    conn.close()

    nodes = []
    links = []
    node_map = {}

    def node_idx(name):
        if name not in node_map:
            node_map[name] = len(nodes)
            nodes.append({"name": name})
        return node_map[name]

    for row in scheme_rows:
        dn = row["dept_name"]
        sn = row["scheme_name"]
        src = node_idx(dn)
        tgt = node_idx(sn)
        links.append({"source": src, "target": tgt, "value": row["allocation"] / 1e8, "category": row["category"]})

    for i, srow in enumerate(scheme_rows[:8]):
        for drow in dist_rows[:4]:
            links.append({
                "source": node_idx(srow["scheme_name"]),
                "target": node_idx(drow["dist_name"]),
                "value": drow["dist_util"] / 1e9 * (0.3 + i * 0.05),
                "category": "district"
            })

    return {"nodes": nodes, "links": links}


def get_district_performance():
    """Get district performance metrics."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT d.*, COUNT(t.id) as tx_count, SUM(t.amount) as tx_total,
               SUM(CASE WHEN t.flagged=1 THEN 1 ELSE 0 END) as flagged_count
        FROM districts d
        LEFT JOIN transactions t ON t.district_id = d.id
        GROUP BY d.id
        ORDER BY d.performance_score DESC
    """)
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()

    for row in rows:
        if row["allocation"]:
            row["utilization_pct"] = round(row["utilized"] / row["allocation"] * 100, 1)
        else:
            row["utilization_pct"] = 0
    return rows
