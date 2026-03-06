"""
BudgetFlow Intelligence — Backend Server
HTTP server + 18 REST API endpoints (Python stdlib only).
Run:  python main.py
"""
import http.server
import json
import os
import re
import sys
import traceback
import urllib.parse
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

from database import initialize_db, get_connection
from analytics.anomaly import AnomalyDetector
from analytics.predictor import Predictor
from analytics.optimizer import Optimizer

FRONTEND_DIR = os.path.normpath(os.path.join(BASE_DIR, "..", "frontend"))

MIME_TYPES = {
    ".html": "text/html; charset=utf-8",
    ".css":  "text/css; charset=utf-8",
    ".js":   "application/javascript; charset=utf-8",
    ".json": "application/json",
    ".ico":  "image/x-icon",
    ".png":  "image/png",
    ".jpg":  "image/jpeg",
    ".svg":  "image/svg+xml",
}


# =========================================================================== #
#  Request Handler                                                             #
# =========================================================================== #

class BudgetFlowHandler(http.server.BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):
        print(f"  [{self.date_time_string()}] {fmt % args}")

    # ----------------------------------------------------------------------- #
    #  CORS / response helpers                                                 #
    # ----------------------------------------------------------------------- #

    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def send_json(self, data, status=200):
        body = json.dumps(data, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self._cors()
        self.end_headers()
        self.wfile.write(body)

    def send_err(self, message, status=400):
        self.send_json({"status": "error", "message": message}, status)

    # ----------------------------------------------------------------------- #
    #  Routing                                                                 #
    # ----------------------------------------------------------------------- #

    def do_OPTIONS(self):
        self.send_response(200)
        self._cors()
        self.end_headers()

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path   = parsed.path
        query  = urllib.parse.parse_qs(parsed.query)
        if path.startswith("/api/"):
            self._route_get(path, query)
        else:
            self._serve_static(path)

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        path   = parsed.path
        length = int(self.headers.get("Content-Length", 0))
        raw    = self.rfile.read(length) if length > 0 else b"{}"
        try:
            body = json.loads(raw) if raw.strip() else {}
        except json.JSONDecodeError:
            body = {}
        self._route_post(path, body)

    # ----------------------------------------------------------------------- #
    #  Static file server                                                      #
    # ----------------------------------------------------------------------- #

    def _serve_static(self, path):
        if path in ("/", ""):
            path = "/index.html"
        candidate = os.path.normpath(os.path.join(FRONTEND_DIR, path.lstrip("/")))
        # Security: prevent directory traversal
        if not candidate.startswith(os.path.normpath(FRONTEND_DIR)):
            self.send_response(403)
            self.end_headers()
            return
        if not os.path.isfile(candidate):
            candidate = os.path.join(FRONTEND_DIR, "index.html")
        if not os.path.isfile(candidate):
            self.send_response(404)
            self.end_headers()
            return
        ext  = os.path.splitext(candidate)[1].lower()
        mime = MIME_TYPES.get(ext, "application/octet-stream")
        with open(candidate, "rb") as f:
            data = f.read()
        self.send_response(200)
        self.send_header("Content-Type", mime)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    # ----------------------------------------------------------------------- #
    #  GET routing                                                             #
    # ----------------------------------------------------------------------- #

    def _route_get(self, path, query):
        try:
            conn = get_connection()
            if   path == "/api/overview":                self._api_overview(conn, query)
            elif path == "/api/departments":             self._api_departments(conn, query)
            elif path == "/api/schemes":                 self._api_schemes(conn, query)
            elif path == "/api/districts":               self._api_districts(conn, query)
            elif path == "/api/transactions":            self._api_transactions(conn, query)
            elif path == "/api/anomalies":               self._api_anomalies(conn, query)
            elif re.match(r"^/api/anomalies/[A-Z0-9]+$", path):
                self._api_anomaly_detail(conn, path.split("/")[-1])
            elif path == "/api/risk-matrix":             self._api_risk_matrix(conn, query)
            elif path == "/api/forecast":                self._api_forecast(conn, query)
            elif path == "/api/optimizer":               self._api_optimizer(conn, query)
            elif path == "/api/fund-flow":               self._api_fund_flow(conn, query)
            elif path == "/api/districts/performance":   self._api_districts_perf(conn, query)
            elif path == "/api/audit-log":               self._api_audit_log(conn, query)
            elif path == "/api/alerts":                  self._api_alerts(conn, query)
            elif path == "/api/utilization":             self._api_utilization(conn, query)
            elif path == "/api/benchmarks":              self._api_benchmarks(conn, query)
            else:
                self.send_err("Endpoint not found", 404)
            conn.close()
        except Exception as exc:
            traceback.print_exc()
            self.send_err(f"Server error: {exc}", 500)

    # ----------------------------------------------------------------------- #
    #  POST routing                                                            #
    # ----------------------------------------------------------------------- #

    def _route_post(self, path, body):
        try:
            conn = get_connection()
            if   path == "/api/scan":             self._api_scan(conn, body)
            elif path == "/api/optimizer/apply":  self._api_optimizer_apply(conn, body)
            else:
                self.send_err("Endpoint not found", 404)
            conn.close()
        except Exception as exc:
            traceback.print_exc()
            self.send_err(f"Server error: {exc}", 500)

    # =========================================================================
    #  Endpoint Implementations  (18 total)
    # =========================================================================

    # 1 — Overview KPIs
    def _api_overview(self, conn, query):
        fy = query.get("fy", ["FY2025"])[0]

        total_alloc = conn.execute(
            "SELECT COALESCE(SUM(total_allocation),0) as v FROM schemes"
        ).fetchone()["v"]

        row = conn.execute(
            "SELECT COALESCE(SUM(allocated_amount),0) as a, COALESCE(SUM(utilized_amount),0) as u "
            "FROM allocations WHERE fiscal_year_id=?", (fy,)
        ).fetchone()
        alloc_fy, util_fy = row["a"], row["u"]
        util_pct = round(util_fy / alloc_fy * 100, 1) if alloc_fy else 0

        open_anom    = conn.execute("SELECT COUNT(*) c FROM anomalies WHERE status='open'").fetchone()["c"]
        critical_cnt = conn.execute("SELECT COUNT(*) c FROM anomalies WHERE status='open' AND severity='critical'").fetchone()["c"]
        scheme_cnt   = conn.execute("SELECT COUNT(*) c FROM schemes").fetchone()["c"]
        dept_cnt     = conn.execute("SELECT COUNT(*) c FROM departments").fetchone()["c"]

        quarterly = [dict(r) for r in conn.execute(
            "SELECT quarter, SUM(allocated_amount) allocated, SUM(utilized_amount) utilized "
            "FROM allocations WHERE fiscal_year_id=? GROUP BY quarter ORDER BY quarter", (fy,)
        ).fetchall()]

        top_depts = [dict(r) for r in conn.execute(
            """SELECT d.name,d.short_name,
                      COALESCE(SUM(a.allocated_amount),0) allocated,
                      COALESCE(SUM(a.utilized_amount),0) utilized,
                      ROUND(COALESCE(SUM(a.utilized_amount),0)*100.0/NULLIF(COALESCE(SUM(a.allocated_amount),0),0),1) util_pct
               FROM departments d
               LEFT JOIN schemes s ON s.dept_id=d.id
               LEFT JOIN allocations a ON a.scheme_id=s.id AND a.fiscal_year_id=?
               GROUP BY d.id ORDER BY util_pct DESC""", (fy,)
        ).fetchall()]

        recent_anoms = [dict(r) for r in conn.execute(
            """SELECT a.id,a.anomaly_type,a.severity,a.score,a.description,a.detected_at,
                      d.name dept_name, s.name scheme_name
               FROM anomalies a
               JOIN departments d ON a.dept_id=d.id
               JOIN schemes s ON a.scheme_id=s.id
               WHERE a.status='open' ORDER BY a.score DESC LIMIT 5"""
        ).fetchall()]

        self.send_json({
            "status": "ok",
            "data": {
                "kpis": {
                    "total_allocation": round(total_alloc, 2),
                    "total_utilized":   round(util_fy, 2),
                    "utilization_pct":  util_pct,
                    "active_anomalies": open_anom,
                    "critical_anomalies": critical_cnt,
                    "active_schemes":   scheme_cnt,
                    "active_departments": dept_cnt,
                    "fiscal_year": fy,
                },
                "quarterly_utilization": quarterly,
                "top_departments": top_depts,
                "recent_anomalies": recent_anoms,
            },
            "metadata": {"generated_at": datetime.now().isoformat()},
        })

    # 2 — Departments
    def _api_departments(self, conn, query):
        fy = query.get("fy", ["FY2025"])[0]
        rows = conn.execute(
            """SELECT d.id,d.code,d.name,d.short_name,d.category,
                      COUNT(DISTINCT s.id) scheme_count,
                      COALESCE(SUM(s.total_allocation),0) total_allocation,
                      COALESCE(SUM(a.allocated_amount),0) allocated_fy,
                      COALESCE(SUM(a.utilized_amount),0) utilized_fy,
                      ROUND(COALESCE(SUM(a.utilized_amount),0)*100.0/
                            NULLIF(COALESCE(SUM(a.allocated_amount),0),0),1) utilization_pct
               FROM departments d
               LEFT JOIN schemes s ON s.dept_id=d.id
               LEFT JOIN allocations a ON a.scheme_id=s.id AND a.fiscal_year_id=?
               GROUP BY d.id ORDER BY d.name""", (fy,)
        ).fetchall()
        self.send_json({"status": "ok", "data": [dict(r) for r in rows],
                        "metadata": {"count": len(rows), "fiscal_year": fy}})

    # 3 — Schemes
    def _api_schemes(self, conn, query):
        fy      = query.get("fy",      ["FY2025"])[0]
        dept_id = query.get("dept_id", [None])[0]
        extra   = "AND s.dept_id=?" if dept_id else ""
        params  = (fy, dept_id) if dept_id else (fy,)
        rows = conn.execute(
            f"""SELECT s.id,s.code,s.name,s.total_allocation,s.category,
                       d.name dept_name, d.short_name dept_short,
                       COALESCE(SUM(a.utilized_amount),0) utilized_fy,
                       COALESCE(SUM(a.allocated_amount),0) allocated_fy,
                       ROUND(COALESCE(SUM(a.utilized_amount),0)*100.0/
                             NULLIF(COALESCE(SUM(a.allocated_amount),0),0),1) utilization_pct
                FROM schemes s
                JOIN departments d ON s.dept_id=d.id
                LEFT JOIN allocations a ON a.scheme_id=s.id AND a.fiscal_year_id=?
                WHERE 1=1 {extra}
                GROUP BY s.id ORDER BY s.total_allocation DESC""", params
        ).fetchall()
        self.send_json({"status": "ok", "data": [dict(r) for r in rows],
                        "metadata": {"count": len(rows), "fiscal_year": fy}})

    # 4 — Districts list
    def _api_districts(self, conn, query):
        fy = query.get("fy", ["FY2025"])[0]
        rows = conn.execute(
            """SELECT d.id,d.code,d.name,d.state,d.district_type,
                      COALESCE(SUM(a.allocated_amount),0) total_allocated,
                      COALESCE(SUM(a.utilized_amount),0) total_utilized,
                      ROUND(COALESCE(SUM(a.utilized_amount),0)*100.0/
                            NULLIF(COALESCE(SUM(a.allocated_amount),0),0),1) utilization_pct
               FROM districts d
               LEFT JOIN allocations a ON a.district_id=d.id AND a.fiscal_year_id=?
               GROUP BY d.id ORDER BY d.state,d.name""", (fy,)
        ).fetchall()
        self.send_json({"status": "ok", "data": [dict(r) for r in rows],
                        "metadata": {"count": len(rows), "fiscal_year": fy}})

    # 5 — Transactions (paginated)
    def _api_transactions(self, conn, query):
        fy        = query.get("fy",        ["FY2025"])[0]
        limit_n   = int(query.get("limit", [50])[0])
        offset_n  = int(query.get("offset",[0])[0])
        scheme_id = query.get("scheme_id", [None])[0]
        fltr  = "AND t.scheme_id=?" if scheme_id else ""
        cp    = (fy, scheme_id) if scheme_id else (fy,)
        lp    = cp + (limit_n, offset_n)
        total = conn.execute(
            f"SELECT COUNT(*) c FROM transactions t WHERE t.fiscal_year_id=? {fltr}", cp
        ).fetchone()["c"]
        rows = conn.execute(
            f"""SELECT t.id,t.amount,t.txn_date,t.txn_type,t.vendor,t.description,
                       s.name scheme_name, di.name district_name, di.state
                FROM transactions t
                JOIN schemes s ON t.scheme_id=s.id
                JOIN districts di ON t.district_id=di.id
                WHERE t.fiscal_year_id=? {fltr}
                ORDER BY t.txn_date DESC LIMIT ? OFFSET ?""", lp
        ).fetchall()
        self.send_json({"status": "ok", "data": [dict(r) for r in rows],
                        "metadata": {"total": total, "limit": limit_n,
                                     "offset": offset_n, "fiscal_year": fy}})

    # 6 — Anomaly list
    def _api_anomalies(self, conn, query):
        status   = query.get("status",   ["open"])[0]
        severity = query.get("severity", [None])[0]
        conds  = ["a.status=?"]
        params = [status]
        if severity:
            conds.append("a.severity=?")
            params.append(severity)
        where = "WHERE " + " AND ".join(conds)
        rows = conn.execute(
            f"""SELECT a.id,a.txn_id,a.anomaly_type,a.severity,a.score,
                       a.description,a.status,a.detected_at,
                       d.name dept_name, s.name scheme_name
                FROM anomalies a
                JOIN departments d ON a.dept_id=d.id
                JOIN schemes s ON a.scheme_id=s.id
                {where} ORDER BY a.score DESC,a.detected_at DESC""", params
        ).fetchall()
        self.send_json({"status": "ok", "data": [dict(r) for r in rows],
                        "metadata": {"count": len(rows), "status_filter": status}})

    # 7 — Anomaly detail
    def _api_anomaly_detail(self, conn, anomaly_id):
        row = conn.execute(
            """SELECT a.*,d.name dept_name,s.name scheme_name,s.category
               FROM anomalies a
               JOIN departments d ON a.dept_id=d.id
               JOIN schemes s ON a.scheme_id=s.id
               WHERE a.id=?""", (anomaly_id,)
        ).fetchone()
        if not row:
            self.send_err("Anomaly not found", 404)
            return
        self.send_json({"status": "ok", "data": dict(row)})

    # 8 — Risk matrix
    def _api_risk_matrix(self, conn, query):
        fy = query.get("fy", ["FY2025"])[0]
        rows = conn.execute(
            """SELECT r.*,d.name dept_name,d.short_name,s.name scheme_name
               FROM risk_scores r
               JOIN departments d ON r.dept_id=d.id
               JOIN schemes s ON r.scheme_id=s.id
               WHERE r.fiscal_year_id=? ORDER BY r.composite_score DESC""", (fy,)
        ).fetchall()
        self.send_json({"status": "ok", "data": [dict(r) for r in rows],
                        "metadata": {"count": len(rows), "fiscal_year": fy}})

    # 9 — Forecast
    def _api_forecast(self, conn, query):
        fy = query.get("fy", ["FY2025"])[0]
        rows = conn.execute(
            """SELECT s.dept_id, d.name dept_name, d.short_name,
                      a.quarter,
                      SUM(a.utilized_amount) utilized,
                      SUM(a.allocated_amount) allocated
               FROM allocations a
               JOIN schemes s ON a.scheme_id=s.id
               JOIN departments d ON s.dept_id=d.id
               WHERE a.fiscal_year_id=?
               GROUP BY s.dept_id,a.quarter ORDER BY s.dept_id,a.quarter""", (fy,)
        ).fetchall()
        dept_map = {}
        for r in rows:
            key = r["dept_id"]
            if key not in dept_map:
                dept_map[key] = {"dept_id": key, "dept_name": r["dept_name"],
                                 "short_name": r["short_name"], "quarterly": []}
            dept_map[key]["quarterly"].append({
                "quarter": r["quarter"], "utilized": r["utilized"], "allocated": r["allocated"]
            })
        predictor = Predictor()
        forecasts = [predictor.forecast_department(d) for d in dept_map.values()]
        self.send_json({"status": "ok", "data": forecasts,
                        "metadata": {"count": len(forecasts), "fiscal_year": fy}})

    # 10 — Optimizer recommendations
    def _api_optimizer(self, conn, query):
        fy = query.get("fy", ["FY2025"])[0]
        rows = conn.execute(
            """SELECT s.id,s.name,s.dept_id,s.total_allocation,s.category,
                      d.name dept_name,
                      COALESCE(SUM(a.utilized_amount),0) utilized,
                      COALESCE(SUM(a.allocated_amount),0) allocated
               FROM schemes s
               JOIN departments d ON s.dept_id=d.id
               LEFT JOIN allocations a ON a.scheme_id=s.id AND a.fiscal_year_id=?
               GROUP BY s.id""", (fy,)
        ).fetchall()
        optimizer = Optimizer()
        recs = optimizer.generate_recommendations([dict(r) for r in rows])
        self.send_json({"status": "ok", "data": recs,
                        "metadata": {"count": len(recs), "fiscal_year": fy}})

    # 11 — Fund flow (Sankey data)
    def _api_fund_flow(self, conn, query):
        fy = query.get("fy", ["FY2025"])[0]
        dept_nodes = [{"id": r["id"], "name": r["name"], "type": "department"}
                      for r in conn.execute("SELECT id,name FROM departments").fetchall()]
        scheme_nodes = [{"id": r["id"], "name": r["name"], "type": "scheme"}
                        for r in conn.execute("SELECT id,name FROM schemes").fetchall()]
        links = [dict(r) for r in conn.execute(
            """SELECT d.name source, s.name target,
                      COALESCE(SUM(a.utilized_amount),0) value
               FROM schemes s
               JOIN departments d ON s.dept_id=d.id
               LEFT JOIN allocations a ON a.scheme_id=s.id AND a.fiscal_year_id=?
               GROUP BY d.id,s.id HAVING value>0 ORDER BY value DESC LIMIT 40""", (fy,)
        ).fetchall()]
        self.send_json({"status": "ok",
                        "data": {"nodes": dept_nodes + scheme_nodes, "links": links}})

    # 12 — Districts performance
    def _api_districts_perf(self, conn, query):
        fy    = query.get("fy",    ["FY2025"])[0]
        state = query.get("state", [None])[0]
        extra  = "AND d.state=?" if state else ""
        params = (fy, state) if state else (fy,)
        rows = conn.execute(
            f"""SELECT d.id,d.name,d.state,d.district_type,
                       COALESCE(SUM(a.allocated_amount),0) allocated,
                       COALESCE(SUM(a.utilized_amount),0) utilized,
                       ROUND(COALESCE(SUM(a.utilized_amount),0)*100.0/
                             NULLIF(COALESCE(SUM(a.allocated_amount),0),0),1) utilization_pct
                FROM districts d
                LEFT JOIN allocations a ON a.district_id=d.id AND a.fiscal_year_id=?
                WHERE 1=1 {extra}
                GROUP BY d.id ORDER BY utilization_pct DESC""", params
        ).fetchall()
        states = [r["state"] for r in conn.execute(
            "SELECT DISTINCT state FROM districts ORDER BY state").fetchall()]
        self.send_json({"status": "ok", "data": [dict(r) for r in rows],
                        "metadata": {"count": len(rows), "states": states, "fiscal_year": fy}})

    # 13 — Audit log
    def _api_audit_log(self, conn, query):
        limit_n = int(query.get("limit", [50])[0])
        rows = conn.execute(
            "SELECT * FROM audit_log ORDER BY performed_at DESC LIMIT ?", (limit_n,)
        ).fetchall()
        self.send_json({"status": "ok", "data": [dict(r) for r in rows],
                        "metadata": {"count": len(rows)}})

    # 14 — Alerts
    def _api_alerts(self, conn, query):
        rows = conn.execute(
            """SELECT a.id,a.anomaly_type,a.severity,a.score,a.description,a.detected_at,
                      d.name dept_name,s.name scheme_name
               FROM anomalies a
               JOIN departments d ON a.dept_id=d.id
               JOIN schemes s ON a.scheme_id=s.id
               WHERE a.status='open' AND a.severity IN('critical','high')
               ORDER BY a.score DESC LIMIT 20"""
        ).fetchall()
        self.send_json({"status": "ok", "data": [dict(r) for r in rows],
                        "metadata": {"count": len(rows)}})

    # 15 — Utilization breakdown
    def _api_utilization(self, conn, query):
        fy = query.get("fy", ["FY2025"])[0]
        quarterly = [dict(r) for r in conn.execute(
            """SELECT quarter,
                      SUM(allocated_amount) allocated, SUM(utilized_amount) utilized,
                      ROUND(SUM(utilized_amount)*100.0/NULLIF(SUM(allocated_amount),0),1) pct
               FROM allocations WHERE fiscal_year_id=? GROUP BY quarter""", (fy,)
        ).fetchall()]
        by_cat = [dict(r) for r in conn.execute(
            """SELECT s.category,
                      SUM(a.allocated_amount) allocated, SUM(a.utilized_amount) utilized,
                      ROUND(SUM(a.utilized_amount)*100.0/NULLIF(SUM(a.allocated_amount),0),1) pct
               FROM allocations a JOIN schemes s ON a.scheme_id=s.id
               WHERE a.fiscal_year_id=? GROUP BY s.category ORDER BY pct DESC""", (fy,)
        ).fetchall()]
        self.send_json({"status": "ok",
                        "data": {"quarterly": quarterly, "by_category": by_cat}})

    # 16 — Benchmarks
    def _api_benchmarks(self, conn, query):
        fy = query.get("fy", ["FY2025"])[0]
        rows = conn.execute(
            """SELECT state,metric_name,
                      ROUND(AVG(metric_value),4) avg_value,
                      ROUND(AVG(benchmark_value),4) benchmark,
                      ROUND((AVG(metric_value)-AVG(benchmark_value))/AVG(benchmark_value)*100,1) variance_pct
               FROM benchmarks WHERE fiscal_year_id=?
               GROUP BY state,metric_name ORDER BY state,metric_name""", (fy,)
        ).fetchall()
        self.send_json({"status": "ok", "data": [dict(r) for r in rows]})

    # 17 — Anomaly scan (POST)
    def _api_scan(self, conn, body):
        fy = body.get("fiscal_year", "FY2025")
        conn.execute("DELETE FROM anomalies WHERE status='open'")
        conn.execute("DELETE FROM risk_scores WHERE fiscal_year_id=?", (fy,))

        txns = conn.execute(
            """SELECT t.*,s.dept_id,s.total_allocation,s.category,d.name dept_name
               FROM transactions t
               JOIN schemes s ON t.scheme_id=s.id
               JOIN departments d ON s.dept_id=d.id
               WHERE t.fiscal_year_id=?""", (fy,)
        ).fetchall()

        detector = AnomalyDetector()
        anomalies, risk_scores = detector.scan([dict(t) for t in txns], fy)

        for a in anomalies:
            conn.execute(
                "INSERT OR REPLACE INTO anomalies VALUES (?,?,?,?,?,?,?,?,?,?)",
                (a["id"], a.get("txn_id"), a["scheme_id"], a["dept_id"],
                 a["anomaly_type"], a["severity"], a["score"],
                 a["description"], "open", a["detected_at"])
            )
        for r in risk_scores:
            conn.execute(
                "INSERT OR REPLACE INTO risk_scores VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (r["id"], r["dept_id"], r["scheme_id"], fy,
                 r["composite_score"], r["zscore_flag"], r["benford_flag"],
                 r["salami_flag"], r["yearend_flag"], r["lapse_probability"],
                 r["calculated_at"])
            )

        n = conn.execute("SELECT COUNT(*) c FROM audit_log").fetchone()["c"]
        conn.execute("INSERT INTO audit_log VALUES (?,?,?,?,?,?,?)",
                     (f"AUD{n:05d}", "scan", "anomaly_detection", "SCAN001",
                      f"Scan: {len(anomalies)} anomalies, {len(risk_scores)} risk scores",
                      "system", datetime.now().isoformat()))
        conn.commit()

        self.send_json({
            "status": "ok",
            "data": {
                "anomalies_detected": len(anomalies),
                "risk_scores_updated": len(risk_scores),
                "critical": sum(1 for a in anomalies if a["severity"] == "critical"),
                "high":     sum(1 for a in anomalies if a["severity"] == "high"),
                "medium":   sum(1 for a in anomalies if a["severity"] == "medium"),
            },
        })

    # 18 — Apply reallocation (POST)
    def _api_optimizer_apply(self, conn, body):
        rec_id = body.get("recommendation_id")
        if not rec_id:
            self.send_err("recommendation_id is required", 400)
            return
        n = conn.execute("SELECT COUNT(*) c FROM audit_log").fetchone()["c"]
        conn.execute("INSERT INTO audit_log VALUES (?,?,?,?,?,?,?)",
                     (f"AUD{n:05d}", "apply_reallocation", "recommendation", rec_id,
                      f"Reallocation {rec_id} approved and applied",
                      "admin", datetime.now().isoformat()))
        conn.commit()
        self.send_json({"status": "ok",
                        "data": {"message": f"Recommendation {rec_id} applied successfully"}})


# =========================================================================== #
#  Server bootstrap                                                            #
# =========================================================================== #

def _run_initial_scan():
    """Auto-run anomaly scan if the DB has no anomalies yet."""
    try:
        conn = get_connection()
        if conn.execute("SELECT COUNT(*) c FROM anomalies").fetchone()["c"] > 0:
            conn.close()
            return
        fy = "FY2025"
        txns = conn.execute(
            """SELECT t.*,s.dept_id,s.total_allocation,s.category,d.name dept_name
               FROM transactions t
               JOIN schemes s ON t.scheme_id=s.id
               JOIN departments d ON s.dept_id=d.id
               WHERE t.fiscal_year_id=?""", (fy,)
        ).fetchall()
        detector = AnomalyDetector()
        anomalies, risk_scores = detector.scan([dict(t) for t in txns], fy)
        for a in anomalies:
            conn.execute(
                "INSERT OR REPLACE INTO anomalies VALUES (?,?,?,?,?,?,?,?,?,?)",
                (a["id"], a.get("txn_id"), a["scheme_id"], a["dept_id"],
                 a["anomaly_type"], a["severity"], a["score"],
                 a["description"], "open", a["detected_at"])
            )
        for r in risk_scores:
            conn.execute(
                "INSERT OR REPLACE INTO risk_scores VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (r["id"], r["dept_id"], r["scheme_id"], fy,
                 r["composite_score"], r["zscore_flag"], r["benford_flag"],
                 r["salami_flag"], r["yearend_flag"], r["lapse_probability"],
                 r["calculated_at"])
            )
        conn.commit()
        conn.close()
        print(f"[INIT] Auto-scan: {len(anomalies)} anomalies | {len(risk_scores)} risk scores")
    except Exception as e:
        print(f"[WARN] Initial scan failed: {e}")


def run_server(host="0.0.0.0", port=8000):
    initialize_db()
    _run_initial_scan()
    server = http.server.ThreadingHTTPServer((host, port), BudgetFlowHandler)
    banner = f"""
╔══════════════════════════════════════════╗
║       BudgetFlow Intelligence            ║
║  Server: http://localhost:{port}           ║
║  Press Ctrl+C to stop                    ║
╚══════════════════════════════════════════╝"""
    print(banner)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[INFO] Server stopped.")


if __name__ == "__main__":
    run_server()
