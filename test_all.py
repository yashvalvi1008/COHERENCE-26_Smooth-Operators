"""
BudgetFlow Intelligence — Test Suite
Tests analytics modules and API route handlers (no HTTP server needed).
Run: python tests/test_all.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import json
import traceback

from db.database import initialize_database
initialize_database()  # ensure DB exists before any test runs
from analytics.anomaly_detection import (
    z_score, iqr_bounds, benfords_law_deviation, detect_spending_spike,
    detect_year_end_rush, detect_vendor_concentration, detect_fund_stall,
    detect_duplicate_amounts, ml_anomaly_score, full_department_scan,
)
from analytics.forecasting import (
    linear_regression, exponential_smoothing, moving_average,
    lapse_probability, forecast_monthly_spend, spending_trend_analysis,
)
from analytics.optimization import (
    compute_department_states, greedy_reallocation,
    simulate_reallocation, run_optimization, OptimizationConstraints,
)
from api.routes import (
    get_health, get_overview, get_departments, get_department,
    get_anomalies, get_forecast, get_reallocation_recommendations,
    get_districts, get_fund_flow, get_schemes, run_anomaly_scan,
)

PASS = "✅"
FAIL = "❌"
results = []


def test(name, fn):
    try:
        fn()
        results.append((PASS, name))
        print(f"  {PASS}  {name}")
    except AssertionError as e:
        results.append((FAIL, name))
        print(f"  {FAIL}  {name}: {e}")
    except Exception as e:
        results.append((FAIL, name))
        print(f"  {FAIL}  {name}: {type(e).__name__}: {e}")
        traceback.print_exc()


# ──────────────────────────────────────────────
print("\n── ANALYTICS: Anomaly Detection ─────────────────")

def test_z_score():
    zs = z_score([10, 12, 11, 13, 12, 11, 10, 12, 200])
    assert len(zs) == 9
    assert zs[-1] > 2.0, f"Expected last z > 2, got {zs[3]}"
test("Z-score calculation", test_z_score)

def test_iqr():
    lo, hi = iqr_bounds([10,12,11,13,14,100])
    assert hi < 100, f"Upper bound should exclude outlier, got {hi}"
test("IQR bounds", test_iqr)

def test_benford():
    # Benford-conforming data: 1st digits weighted toward 1, 2, 3
    import random
    random.seed(1)
    vals = [random.uniform(100, 999) for _ in range(200)]
    chi = benfords_law_deviation(vals)
    assert isinstance(chi, float) and chi >= 0
test("Benford's law deviation", test_benford)

def test_spending_spike():
    monthly = [50, 55, 48, 60, 52, 58, 55, 62, 200, 58, 60, 65]
    alerts = detect_spending_spike(monthly, threshold_z=1.5)
    assert len(alerts) >= 1, f"Expected spike at month 9, got {alerts}"
    assert alerts[0]["type"] == "spending_spike"
test("Spending spike detection", test_spending_spike)

def test_year_end_rush():
    # 60% of spending in Q4
    monthly = [5]*9 + [40, 50, 60]
    result = detect_year_end_rush(monthly, q4_threshold=0.4)
    assert result is not None, "Should detect year-end rush"
    assert result["type"] == "year_end_rush"
    assert result["severity"] in ("critical", "high")
test("Year-end rush detection", test_year_end_rush)

def test_vendor_concentration():
    vendors = {"VND001": 700.0, "VND002": 150.0, "VND003": 150.0}
    result = detect_vendor_concentration(vendors, 0.5)
    assert result is not None
    assert result["vendor_id"] == "VND001"
    assert result["share"] > 0.5
test("Vendor concentration detection", test_vendor_concentration)

def test_fund_stall():
    result = detect_fund_stall(500, 5, 270)  # 500 alloc, 5 spent
    assert result is not None, "Should detect stall"
    assert result["type"] == "fund_stall"
test("Fund stall detection", test_fund_stall)

def test_duplicate_amounts():
    amounts = [100.5, 200.0, 100.5, 100.5, 300.0, 100.5, 400.0]
    alerts = detect_duplicate_amounts(amounts)
    assert len(alerts) >= 1
    assert alerts[0]["amount_cr"] == 100.5
    assert alerts[0]["occurrences"] == 4
test("Duplicate amount detection", test_duplicate_amounts)

def test_ml_score():
    score = ml_anomaly_score({
        "utilization_gap": 0.4,
        "spending_velocity": 1.5,
        "vendor_concentration": 0.7,
        "year_end_ratio": 0.5,
        "benford_deviation": 0.3,
        "duplicate_ratio": 0.2,
        "stall_flag": 0,
    })
    assert 0 <= score <= 100, f"Score out of range: {score}"
    assert score > 50, f"Should be high risk, got {score}"
test("ML anomaly score", test_ml_score)

def test_full_scan():
    monthly = [5, 8, 12, 15, 18, 22, 28, 35, 42, 60, 80, 120]
    vendors = {"VND1": 800, "VND2": 100, "VND3": 100}
    result = full_department_scan("social", monthly, vendors, 1000, 270)
    assert "ml_score" in result
    assert "alerts" in result
    assert isinstance(result["alerts"], list)
test("Full department scan", test_full_scan)


# ──────────────────────────────────────────────
print("\n── ANALYTICS: Forecasting ───────────────────────")

def test_linear_regression():
    x = [1, 2, 3, 4, 5]
    y = [2, 4, 5, 4, 5]
    slope, intercept, r_sq = linear_regression(x, y)
    assert isinstance(slope, float)
    assert 0 <= r_sq <= 1, f"R² should be 0-1, got {r_sq}"
test("Linear regression", test_linear_regression)

def test_exp_smoothing():
    vals = [10, 12, 11, 15, 14, 18]
    smoothed = exponential_smoothing(vals, 0.3)
    assert len(smoothed) == len(vals)
    assert smoothed[0] == vals[0]
test("Exponential smoothing", test_exp_smoothing)

def test_lapse_probability_low():
    # Well-performing dept: should have low lapse risk
    result = lapse_probability(1000, 800, 270)
    assert result["probability"] < 0.5, f"Expected low risk, got {result['probability']}"
test("Lapse probability — low risk dept", test_lapse_probability_low)

def test_lapse_probability_high():
    # Poorly performing dept
    result = lapse_probability(1000, 200, 270)
    assert result["probability"] > 0.5, f"Expected high risk, got {result['probability']}"
    assert result["lapse_amount_cr"] > 0
test("Lapse probability — high risk dept", test_lapse_probability_high)

def test_forecast():
    monthly = [50, 55, 60, 65, 70, 75, 80, 85, 90]
    forecasts = forecast_monthly_spend(monthly, periods_ahead=3)
    assert len(forecasts) == 3
    for f in forecasts:
        assert "forecast" in f
        assert f["lower_ci"] <= f["forecast"] <= f["upper_ci"]
test("Monthly spend forecast (ES)", test_forecast)

def test_trend_analysis():
    monthly = [30, 35, 40, 45, 50, 55, 60, 70, 80]
    result = spending_trend_analysis(monthly)
    assert result["trend_direction"] in ("increasing", "decreasing", "flat")
    assert "volatility" in result
    assert "smoothed" in result
test("Spending trend analysis", test_trend_analysis)


# ──────────────────────────────────────────────
print("\n── ANALYTICS: Optimization ──────────────────────")

SAMPLE_DEPTS = [
    {"id": "health",  "name": "Health",   "allocated_cr": 62400, "spent_cr": 43680},
    {"id": "edu",     "name": "Edu",      "allocated_cr": 54800, "spent_cr": 44388},
    {"id": "infra",   "name": "Infra",    "allocated_cr": 89600, "spent_cr": 58240},
    {"id": "social",  "name": "Social",   "allocated_cr": 44100, "spent_cr": 22050},
    {"id": "water",   "name": "Water",    "allocated_cr": 29500, "spent_cr": 24885},
]

def test_dept_states():
    states = compute_department_states(SAMPLE_DEPTS, 270)
    assert len(states) == len(SAMPLE_DEPTS)
    for s in states:
        assert s["classification"] in ("surplus", "deficit", "balanced")
        assert "urgency" in s
test("Department state classification", test_dept_states)

def test_greedy_optimizer():
    states = compute_department_states(SAMPLE_DEPTS, 270)
    constraints = OptimizationConstraints()
    recs = greedy_reallocation(states, constraints)
    assert isinstance(recs, list)
    for r in recs:
        assert "from_dept_id" in r
        assert "to_dept_id" in r
        assert r["amount_cr"] > 0
        assert r["priority"] in ("critical", "high", "medium", "low")
test("Greedy reallocation optimizer", test_greedy_optimizer)

def test_simulation():
    states = compute_department_states(SAMPLE_DEPTS, 270)
    constraints = OptimizationConstraints()
    recs = greedy_reallocation(states, constraints)
    sim = simulate_reallocation(states, recs)
    assert "total_transferred_cr" in sim
    assert "before_avg_utilization_pct" in sim
    assert "after_avg_utilization_pct" in sim
test("Reallocation simulation", test_simulation)

def test_full_optimization():
    result = run_optimization(SAMPLE_DEPTS)
    assert "recommendations" in result
    assert "simulation" in result
    assert "summary" in result
    assert result["summary"]["total_recommendations"] >= 0
test("Full optimization pipeline", test_full_optimization)


# ──────────────────────────────────────────────
print("\n── API: Route Handlers ──────────────────────────")

def test_api_health():
    resp = get_health()
    assert resp["status"] == "success"
    assert resp["data"]["status"] == "healthy"
test("GET /api/health", test_api_health)

def test_api_overview():
    resp = get_overview()
    assert resp["status"] == "success"
    data = resp["data"]
    assert "kpis" in data
    assert "departments" in data
    assert data["kpis"]["total_allocation_cr"] > 0
test("GET /api/overview", test_api_overview)

def test_api_departments():
    resp = get_departments()
    assert resp["status"] == "success"
    assert len(resp["data"]) > 0
    dept = resp["data"][0]
    assert "allocated_cr" in dept
    assert "lapse_risk" in dept
test("GET /api/departments", test_api_departments)

def test_api_single_dept():
    resp = get_department("health")
    assert resp["status"] == "success"
    assert resp["data"]["id"] == "health"
    assert "anomaly_scan" in resp["data"]
    assert "forecast" in resp["data"]
test("GET /api/departments/health", test_api_single_dept)

def test_api_dept_not_found():
    resp = get_department("nonexistent_dept_xyz")
    assert resp["status"] == "error"
    assert resp["code"] == 404
test("GET /api/departments/{bad_id} → 404", test_api_dept_not_found)

def test_api_anomalies():
    resp = get_anomalies()
    assert resp["status"] == "success"
    assert isinstance(resp["data"], list)
    assert "stats" in resp.get("meta", {})
test("GET /api/anomalies", test_api_anomalies)

def test_api_anomalies_filtered():
    resp = get_anomalies(severity="critical")
    assert resp["status"] == "success"
    for a in resp["data"]:
        assert a["severity"] == "critical"
test("GET /api/anomalies?severity=critical", test_api_anomalies_filtered)

def test_api_forecast():
    resp = get_forecast()
    assert resp["status"] == "success"
    assert "department_risks" in resp["data"]
    assert "national_summary" in resp["data"]
    for d in resp["data"]["department_risks"]:
        assert 0 <= d["probability"] <= 1
test("GET /api/forecast", test_api_forecast)

def test_api_reallocation():
    resp = get_reallocation_recommendations()
    assert resp["status"] == "success"
    data = resp["data"]
    assert "ai_recommendations" in data
    assert "simulation" in data
    assert "summary" in data
test("GET /api/reallocation", test_api_reallocation)

def test_api_districts():
    resp = get_districts()
    assert resp["status"] == "success"
    assert len(resp["data"]) > 0
    d = resp["data"][0]
    assert "utilization_pct" in d
    assert "risk_level" in d
test("GET /api/districts", test_api_districts)

def test_api_fund_flow():
    resp = get_fund_flow()
    assert resp["status"] == "success"
    data = resp["data"]
    assert "sankey" in data
    assert "nodes" in data["sankey"]
    assert "links" in data["sankey"]
test("GET /api/fund-flow", test_api_fund_flow)

def test_api_schemes():
    resp = get_schemes()
    assert resp["status"] == "success"
    assert len(resp["data"]) > 0
    s = resp["data"][0]
    assert "utilization_pct" in s
test("GET /api/schemes", test_api_schemes)

def test_api_scan():
    resp = run_anomaly_scan()
    assert resp["status"] == "success"
    assert "scan_results" in resp["data"]
    assert resp["data"]["scanned_departments"] > 0
test("GET /api/scan (anomaly scan)", test_api_scan)


# ──────────────────────────────────────────────
print("\n── SUMMARY ──────────────────────────────────────")
passed = sum(1 for r in results if r[0] == PASS)
failed = sum(1 for r in results if r[0] == FAIL)
total = len(results)
print(f"\n  {PASS} Passed: {passed}/{total}")
if failed:
    print(f"  {FAIL} Failed: {failed}/{total}")
    print("\n  Failed tests:")
    for r in results:
        if r[0] == FAIL:
            print(f"    • {r[1]}")
print()
sys.exit(0 if failed == 0 else 1)
