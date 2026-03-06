"""
main.py - Zero-Dependency HTTP Server for BudgetFlow Intelligence
Uses Python standard library only: http.server, json, sqlite3, os, urllib.parse
Run: python main.py
"""

import json
import os
import sys
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

# Initialize database first
from database import init_db, DB_PATH
from analytics import (
    get_overview,
    get_fund_flow,
    run_anomaly_scan,
    get_anomaly_list,
    predict_lapse_risk,
    optimize_reallocations,
    get_district_performance,
    get_connection,
)

STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
PORT = 8080

MIME_TYPES = {
    ".html": "text/html; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".js": "application/javascript; charset=utf-8",
    ".json": "application/json; charset=utf-8",
    ".png": "image/png",
    ".ico": "image/x-icon",
    ".svg": "image/svg+xml",
}


def json_response(data, status=200):
    return status, json.dumps({"status": "ok", "data": data}, default=str)


def error_response(msg, status=400):
    return status, json.dumps({"status": "error", "message": msg})


class BudgetFlowHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        print(f"[{self.log_date_time_string()}] {format % args}")

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)

        # Serve static files
        if not path.startswith("/api"):
            self._serve_static(path)
            return

        # API Routing
        status, body = self._route_api(path, query)
        self._send_json(status, body)

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path
        content_length = int(self.headers.get("Content-Length", 0))
        body_bytes = self.rfile.read(content_length) if content_length > 0 else b"{}"
        try:
            body_data = json.loads(body_bytes.decode("utf-8"))
        except Exception:
            body_data = {}

        status, body = self._route_post(path, body_data)
        self._send_json(status, body)

    def _route_api(self, path, query):
        try:
            # ── Overview & Dashboard ──────────────────────────
            if path == "/api/overview":
                return json_response(get_overview())

            elif path == "/api/fund-flow":
                return json_response(get_fund_flow())

            # ── Departments ───────────────────────────────────
            elif path == "/api/departments":
                conn = get_connection()
                cur = conn.cursor()
                cur.execute("SELECT * FROM departments ORDER BY total_allocation DESC")
                rows = [dict(r) for r in cur.fetchall()]
                conn.close()
                for r in rows:
                    if r["total_allocation"]:
                        r["utilization_pct"] = round(r["utilized"] / r["total_allocation"] * 100, 1)
                return json_response(rows)

            elif path.startswith("/api/departments/"):
                dept_id = path.split("/")[-1]
                conn = get_connection()
                cur = conn.cursor()
                cur.execute("SELECT * FROM departments WHERE id=?", (dept_id,))
                dept = dict(cur.fetchone() or {})
                if not dept:
                    return error_response("Department not found", 404)
                cur.execute("SELECT * FROM schemes WHERE department_id=?", (dept_id,))
                dept["schemes"] = [dict(r) for r in cur.fetchall()]
                cur.execute("""
                    SELECT substr(transaction_date, 1, 7) as month, SUM(amount) as total
                    FROM transactions t JOIN schemes s ON t.scheme_id = s.id
                    WHERE s.department_id=? GROUP BY month ORDER BY month
                """, (dept_id,))
                dept["monthly_spend"] = [dict(r) for r in cur.fetchall()]
                conn.close()
                return json_response(dept)

            # ── Schemes ───────────────────────────────────────
            elif path == "/api/schemes":
                conn = get_connection()
                cur = conn.cursor()
                cur.execute("""
                    SELECT s.*, d.name as dept_name
                    FROM schemes s JOIN departments d ON s.department_id = d.id
                    ORDER BY s.allocation DESC
                """)
                rows = [dict(r) for r in cur.fetchall()]
                conn.close()
                for r in rows:
                    if r["allocation"]:
                        r["utilization_pct"] = round(r["utilized"] / r["allocation"] * 100, 1)
                return json_response(rows)

            # ── Anomalies ─────────────────────────────────────
            elif path == "/api/anomalies":
                severity = query.get("severity", [None])[0]
                limit = int(query.get("limit", [100])[0])
                anomalies = get_anomaly_list(limit=limit, severity=severity)
                return json_response(anomalies)

            elif path == "/api/anomalies/scan":
                result = run_anomaly_scan()
                return json_response(result)

            elif path == "/api/anomalies/stats":
                conn = get_connection()
                cur = conn.cursor()
                cur.execute("""
                    SELECT severity, COUNT(*) as count, AVG(score) as avg_score
                    FROM anomalies GROUP BY severity
                """)
                stats = [dict(r) for r in cur.fetchall()]
                cur.execute("""
                    SELECT type, COUNT(*) as count FROM anomalies GROUP BY type ORDER BY count DESC
                """)
                by_type = [dict(r) for r in cur.fetchall()]
                conn.close()
                return json_response({"by_severity": stats, "by_type": by_type})

            # ── Predictive Risk ───────────────────────────────
            elif path == "/api/risk":
                predictions = predict_lapse_risk()
                return json_response(predictions)

            elif path == "/api/risk/scores":
                conn = get_connection()
                cur = conn.cursor()
                cur.execute("""
                    SELECT rs.*, d.name as dept_name, d.category
                    FROM risk_scores rs JOIN departments d ON rs.department_id = d.id
                    ORDER BY rs.lapse_probability DESC
                """)
                rows = [dict(r) for r in cur.fetchall()]
                conn.close()
                return json_response(rows)

            # ── Optimizer ─────────────────────────────────────
            elif path == "/api/optimizer":
                recommendations = optimize_reallocations()
                return json_response(recommendations)

            elif path == "/api/optimizer/summary":
                recs = optimize_reallocations()
                total_transfer = sum(r["amount"] for r in recs)
                return json_response({
                    "total_recommendations": len(recs),
                    "total_transfer_amount": round(total_transfer, 2),
                    "departments_with_surplus": len(set(r["from_dept_id"] for r in recs)),
                    "departments_with_deficit": len(set(r["to_dept_id"] for r in recs)),
                })

            # ── Districts ─────────────────────────────────────
            elif path == "/api/districts":
                return json_response(get_district_performance())

            elif path.startswith("/api/districts/"):
                dist_id = path.split("/")[-1]
                conn = get_connection()
                cur = conn.cursor()
                cur.execute("SELECT * FROM districts WHERE id=?", (dist_id,))
                dist = dict(cur.fetchone() or {})
                if not dist:
                    return error_response("District not found", 404)
                cur.execute("""
                    SELECT * FROM transactions WHERE district_id=?
                    ORDER BY transaction_date DESC LIMIT 20
                """, (dist_id,))
                dist["transactions"] = [dict(r) for r in cur.fetchall()]
                conn.close()
                return json_response(dist)

            # ── Transactions ──────────────────────────────────
            elif path == "/api/transactions":
                limit = int(query.get("limit", [50])[0])
                flagged_only = query.get("flagged", ["false"])[0].lower() == "true"
                conn = get_connection()
                cur = conn.cursor()
                if flagged_only:
                    cur.execute("SELECT * FROM transactions WHERE flagged=1 ORDER BY amount DESC LIMIT ?", (limit,))
                else:
                    cur.execute("SELECT * FROM transactions ORDER BY transaction_date DESC LIMIT ?", (limit,))
                rows = [dict(r) for r in cur.fetchall()]
                conn.close()
                return json_response(rows)

            # ── Health ────────────────────────────────────────
            elif path == "/api/health":
                return json_response({"status": "healthy", "db": DB_PATH, "version": "1.0.0"})

            else:
                return error_response("Endpoint not found", 404)

        except Exception as e:
            import traceback
            traceback.print_exc()
            return error_response(f"Internal error: {str(e)}", 500)

    def _route_post(self, path, body):
        try:
            if path == "/api/anomalies/resolve":
                anom_id = body.get("anomaly_id")
                if not anom_id:
                    return error_response("anomaly_id required")
                conn = get_connection()
                conn.execute("UPDATE anomalies SET status='Resolved' WHERE id=?", (anom_id,))
                conn.commit()
                conn.close()
                return json_response({"resolved": anom_id})

            elif path == "/api/optimizer/apply":
                rec = body
                conn = get_connection()
                import datetime
                conn.execute("""
                    INSERT OR IGNORE INTO allocations (id, from_dept, to_dept, amount, reason, status, created_date)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    f"ALLOC-{rec.get('from_dept_id','?')}-{rec.get('to_dept_id','?')}",
                    rec.get("from_dept_id"), rec.get("to_dept_id"),
                    rec.get("amount", 0), rec.get("reason", ""),
                    "Applied", str(datetime.date.today())
                ))
                conn.commit()
                conn.close()
                return json_response({"applied": True})

            else:
                return error_response("POST endpoint not found", 404)

        except Exception as e:
            return error_response(str(e), 500)

    def _serve_static(self, path):
        if path == "/" or path == "":
            path = "/index.html"
        file_path = os.path.join(STATIC_DIR, path.lstrip("/").replace("/", os.sep))
        if not os.path.exists(file_path) or not os.path.isfile(file_path):
            self.send_error(404, "File not found")
            return
        ext = os.path.splitext(file_path)[1].lower()
        content_type = MIME_TYPES.get(ext, "application/octet-stream")
        with open(file_path, "rb") as f:
            data = f.read()
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(data)

    def _send_json(self, status, body):
        body_bytes = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body_bytes)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body_bytes)


def run():
    init_db()
    print(f"""
╔══════════════════════════════════════════════════╗
║         BudgetFlow Intelligence v1.0             ║
║         Zero-Dependency Governance Platform      ║
╠══════════════════════════════════════════════════╣
║  Server:   http://localhost:{PORT}                   ║
║  Database: {os.path.basename(DB_PATH)}                      ║
╚══════════════════════════════════════════════════╝
    """)
    server = HTTPServer(("", PORT), BudgetFlowHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[Server] Shutting down.")
        server.server_close()


if __name__ == "__main__":
    run()
