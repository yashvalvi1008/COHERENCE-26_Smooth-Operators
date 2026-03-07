"""
BudgetFlow Intelligence — Main Server
Pure Python HTTP server using http.server + urllib.parse.
No external dependencies required.

Usage:
    python main.py              # runs on http://localhost:8000
    python main.py --port 9000  # custom port
"""

import sys
import os
import json
import argparse
import threading
import traceback
import urllib.parse
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
from datetime import datetime

# ── path setup ──
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

from db.database import initialize_database
from api.routes import (
    get_health, get_overview,
    get_departments, get_department,
    get_anomalies, get_anomaly, update_anomaly_status,
    get_forecast,
    get_reallocation_recommendations, apply_reallocation,
    get_districts, get_district,
    get_fund_flow, get_schemes,
    run_anomaly_scan, get_audit_log,
    post_chat, get_stream_events,
)


# ──────────────────────────────────────────────
# ROUTER
# ──────────────────────────────────────────────

class Router:
    def __init__(self):
        self.routes = []

    def add(self, method: str, path: str, handler):
        self.routes.append((method.upper(), path, handler))

    def match(self, method: str, path: str):
        for route_method, route_path, handler in self.routes:
            if method.upper() != route_method:
                continue
            params = self._match_path(route_path, path)
            if params is not None:
                return handler, params
        return None, {}

    @staticmethod
    def _match_path(pattern: str, path: str):
        pat_parts = pattern.strip('/').split('/')
        path_parts = path.strip('/').split('/')
        if len(pat_parts) != len(path_parts):
            return None
        params = {}
        for p, v in zip(pat_parts, path_parts):
            if p.startswith('{') and p.endswith('}'):
                params[p[1:-1]] = v
            elif p != v:
                return None
        return params


# ──────────────────────────────────────────────
# REQUEST HANDLER
# ──────────────────────────────────────────────

class BudgetFlowHandler(BaseHTTPRequestHandler):

    router = Router()
    _router_built = False

    @classmethod
    def build_router(cls):
        if cls._router_built:
            return
        r = cls.router

        # Health
        r.add("GET",  "/api/health",               lambda p, q, b: get_health())

        # Overview
        r.add("GET",  "/api/overview",              lambda p, q, b: get_overview())

        # Departments
        r.add("GET",  "/api/departments",           lambda p, q, b: get_departments())
        r.add("GET",  "/api/departments/{id}",      lambda p, q, b: get_department(p["id"]))

        # Anomalies
        r.add("GET",  "/api/anomalies",             lambda p, q, b: get_anomalies(
            severity=q.get("severity"),
            dept_id=q.get("dept_id"),
            status=q.get("status"),
            limit=int(q.get("limit", 50))
        ))
        r.add("GET",  "/api/anomalies/{id}",        lambda p, q, b: get_anomaly(int(p["id"])))
        r.add("PATCH","/api/anomalies/{id}/status", lambda p, q, b: update_anomaly_status(int(p["id"]), b))

        # Forecast
        r.add("GET",  "/api/forecast",              lambda p, q, b: get_forecast(q.get("dept_id")))
        r.add("GET",  "/api/forecast/{dept_id}",    lambda p, q, b: get_forecast(p["dept_id"]))

        # Reallocation
        r.add("GET",  "/api/reallocation",          lambda p, q, b: get_reallocation_recommendations())
        r.add("POST", "/api/reallocation",          lambda p, q, b: get_reallocation_recommendations(b))
        r.add("POST", "/api/reallocation/apply",    lambda p, q, b: apply_reallocation(b))

        # Districts
        r.add("GET",  "/api/districts",             lambda p, q, b: get_districts(q.get("state")))
        r.add("GET",  "/api/districts/{id}",        lambda p, q, b: get_district(p["id"]))

        # Fund flow
        r.add("GET",  "/api/fund-flow",             lambda p, q, b: get_fund_flow())

        # Schemes
        r.add("GET",  "/api/schemes",               lambda p, q, b: get_schemes())

        # Analytics
        r.add("POST", "/api/scan",                  lambda p, q, b: run_anomaly_scan(b.get("dept_id")))
        r.add("GET",  "/api/scan",                  lambda p, q, b: run_anomaly_scan(q.get("dept_id")))

        # Audit
        r.add("GET",  "/api/audit",                 lambda p, q, b: get_audit_log(int(q.get("limit", 50))))

        # Chatbot
        r.add("POST", "/api/chat",                  lambda p, q, b: post_chat(b))

        # Kafka SSE stream  (handled specially in _handle — not via send_json)
        r.add("GET",  "/api/stream",                lambda p, q, b: "__SSE__")

        cls._router_built = True

    def log_message(self, fmt, *args):
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"  [{ts}] {fmt % args}")

    def send_json(self, data: dict, status: int = 200):
        body = json.dumps(data, default=str, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type",  "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin",  "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PATCH, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
        self.send_header("X-BudgetFlow-Version", "1.0.0")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin",  "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PATCH, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
        self.end_headers()

    def _parse_request(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        qs = dict(urllib.parse.parse_qsl(parsed.query))
        body = {}
        if self.command in ("POST", "PUT", "PATCH"):
            length = int(self.headers.get("Content-Length", 0))
            if length > 0:
                try:
                    raw = self.rfile.read(length)
                    body = json.loads(raw.decode("utf-8"))
                except:
                    body = {}
        return path, qs, body

    def _handle(self):
        BudgetFlowHandler.build_router()
        path, qs, body = self._parse_request()

        # Root info
        if path in ("/", "/api"):
            return self.send_json({
                "name": "BudgetFlow Intelligence API",
                "version": "1.0.0",
                "description": "National Budget Flow Intelligence & Leakage Detection Platform",
                "endpoints": [
                    "GET  /api/health",
                    "GET  /api/overview",
                    "GET  /api/departments",
                    "GET  /api/departments/{id}",
                    "GET  /api/anomalies[?severity=&dept_id=&status=&limit=]",
                    "GET  /api/anomalies/{id}",
                    "PATCH /api/anomalies/{id}/status",
                    "GET  /api/forecast[?dept_id=]",
                    "GET  /api/forecast/{dept_id}",
                    "GET  /api/reallocation",
                    "POST /api/reallocation",
                    "POST /api/reallocation/apply",
                    "GET  /api/districts[?state=]",
                    "GET  /api/districts/{id}",
                    "GET  /api/fund-flow",
                    "GET  /api/schemes",
                    "GET  /api/scan[?dept_id=]",
                    "POST /api/scan",
                    "GET  /api/audit[?limit=]",
                    "POST /api/chat",
                    "GET  /api/stream  (Kafka SSE live stream)",
                ],
            })

        handler, params = self.router.match(self.command, path)
        if handler is None:
            return self.send_json(
                {"status": "error", "message": f"Route not found: {self.command} {path}", "code": 404},
                status=404
            )

        # ── SSE stream: persistent connection, bypasses send_json ──
        if path == "/api/stream":
            timeout = int(qs.get("timeout", 300))
            get_stream_events(self, timeout=timeout)
            return

        try:
            result = handler(params, qs, body)
            http_status = result.get("code", 200) if result.get("status") == "error" else 200
            self.send_json(result, http_status)
        except Exception as exc:
            tb = traceback.format_exc()
            print(f"\n[ERROR] {exc}\n{tb}")
            self.send_json(
                {"status": "error", "message": str(exc), "code": 500},
                status=500
            )


    def do_GET(self):   self._handle()
    def do_POST(self):  self._handle()
    def do_PATCH(self): self._handle()
    def do_PUT(self):   self._handle()


# ──────────────────────────────────────────────
# STARTUP
# ──────────────────────────────────────────────

def print_banner(port: int):
    print()
    print("  ╔══════════════════════════════════════════════════╗")
    print("  ║   BudgetFlow Intelligence — Backend API          ║")
    print("  ║   National Budget Flow & Leakage Detection       ║")
    print("  ╠══════════════════════════════════════════════════╣")
    print(f"  ║   Server  : http://localhost:{port}                 ║")
    print(f"  ║   API Base: http://localhost:{port}/api             ║")
    print(f"  ║   Health  : http://localhost:{port}/api/health      ║")
    print("  ╚══════════════════════════════════════════════════╝")
    print()
    print("  Available endpoints:")
    endpoints = [
        ("GET",   "/api/overview",         "KPIs, dept breakdown, alerts"),
        ("GET",   "/api/departments",       "All department data + risk"),
        ("GET",   "/api/departments/{id}",  "Single dept deep-dive"),
        ("GET",   "/api/anomalies",         "All anomalies (filterable)"),
        ("PATCH", "/api/anomalies/{id}/status","Update anomaly status"),
        ("GET",   "/api/forecast",          "Lapse risk forecast, all depts"),
        ("GET",   "/api/reallocation",      "AI reallocation recommendations"),
        ("POST",  "/api/reallocation/apply","Execute a fund transfer"),
        ("GET",   "/api/districts",         "District-level data"),
        ("GET",   "/api/fund-flow",         "Sankey + transfer log"),
        ("GET",   "/api/schemes",           "Centrally sponsored schemes"),
        ("GET",   "/api/scan",              "Run live anomaly scan"),
        ("GET",   "/api/audit",             "Audit log"),
    ]
    for method, path, desc in endpoints:
        print(f"    {method:<6}  {path:<35}  {desc}")
    print()
    print("  Press Ctrl+C to stop.\n")


from socketserver import ThreadingMixIn  # ← at the TOP of the file with other imports

class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):  # ← outside run(), at module level
    daemon_threads = True

def run(port: int = 8000):
    initialize_database()
    print_banner(port)
    server = ThreadedHTTPServer(("0.0.0.0", port), BudgetFlowHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  [Server] Shutting down gracefully...")
        server.shutdown()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BudgetFlow Intelligence API Server")
    parser.add_argument("--port", type=int, default=8000, help="Port to listen on (default: 8000)")
    args = parser.parse_args()
    run(args.port)
