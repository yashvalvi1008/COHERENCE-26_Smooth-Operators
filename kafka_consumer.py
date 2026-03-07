"""
BudgetFlow Intelligence — Kafka Consumer & SSE Bridge
Consumes from Kafka topics and maintains an in-memory event queue
that the main server reads for Server-Sent Events (SSE).

Architecture:
    Kafka Broker
        └─ budget.expenditures  ┐
        └─ budget.anomalies     ├─► KafkaConsumerBridge ──► _EVENT_QUEUE ──► /api/stream (SSE)
        └─ budget.transfers     │
        └─ budget.kpi_updates   ┘

Usage (standalone test):
    python kafka_consumer.py --broker localhost:9092
    python kafka_consumer.py --mock    # reads from mock_stream.jsonl

The BudgetFlowHandler in main.py imports and uses:
    from kafka_consumer import get_event_queue, start_consumer_thread
"""

import json
import time
import threading
import argparse
import os
from collections import deque
from datetime import datetime

# ── shared event queue (thread-safe deque, max 500 events) ──
_EVENT_QUEUE: deque = deque(maxlen=500)
_QUEUE_LOCK  = threading.Lock()
_CONSUMER_STARTED = False

# ── SSE subscriber queues (one per connected client) ──
_SSE_CLIENTS: list = []
_CLIENTS_LOCK = threading.Lock()


def get_event_queue() -> deque:
    return _EVENT_QUEUE


def push_event(event: dict):
    """Add event to shared queue and fan-out to all SSE clients."""
    with _QUEUE_LOCK:
        _EVENT_QUEUE.append(event)
    # Fan-out to SSE subscribers
    with _CLIENTS_LOCK:
        dead = []
        for q in _SSE_CLIENTS:
            try:
                q.append(event)
            except Exception:
                dead.append(q)
        for q in dead:
            _SSE_CLIENTS.remove(q)


def register_sse_client() -> deque:
    """Register a new SSE client and return its personal event deque."""
    client_q = deque(maxlen=100)
    with _CLIENTS_LOCK:
        _SSE_CLIENTS.append(client_q)
    return client_q


def unregister_sse_client(client_q: deque):
    with _CLIENTS_LOCK:
        try:
            _SSE_CLIENTS.remove(client_q)
        except ValueError:
            pass


# ── Mock consumer (reads from mock_stream.jsonl, auto-generates if missing) ──

class MockConsumer:
    """
    Reads events from mock_stream.jsonl produced by kafka_producer.py --mock.
    If the file does not exist yet, generates synthetic events internally
    so the SSE stream always has data regardless of Kafka availability.
    """

    def __init__(self, filepath="mock_stream.jsonl", interval=2.0):
        self.filepath = filepath
        self.interval = interval
        self._pos     = 0
        print(f"[MockConsumer] Reading from: {filepath} (or generating synthetic events)")

    def _synthetic_event(self):
        """Generate one synthetic event without needing a file."""
        import random
        DEPTS = [
            {"id": "health",  "name": "Health & Family Welfare",  "color": "#00ff88"},
            {"id": "edu",     "name": "Education",                 "color": "#a855f7"},
            {"id": "infra",   "name": "Infrastructure & Roads",    "color": "#ffb800"},
            {"id": "agri",    "name": "Agriculture & Allied",      "color": "#00d4ff"},
            {"id": "social",  "name": "Social Welfare & Justice",  "color": "#ff3b5c"},
            {"id": "water",   "name": "Water & Sanitation",        "color": "#00d4ff"},
            {"id": "energy",  "name": "Energy & Renewables",       "color": "#ffb800"},
            {"id": "urban",   "name": "Urban Development",         "color": "#a855f7"},
        ]
        DISTRICTS = ["Mumbai", "Lucknow", "Chennai", "Patna",
                     "Bengaluru", "Jaipur", "Bhopal", "Hyderabad"]

        etype = random.choices(
            ["expenditure", "anomaly", "transfer", "kpi_update"],
            weights=[60, 20, 10, 10]
        )[0]

        dept = random.choice(DEPTS)
        now  = datetime.utcnow().isoformat() + "Z"

        if etype == "expenditure":
            return {
                "event_type":    "expenditure",
                "topic":         "budget.expenditures",
                "timestamp":     now,
                "department_id": dept["id"],
                "dept_name":     dept["name"],
                "dept_color":    dept["color"],
                "amount_cr":     round(random.uniform(0.5, 120.0), 2),
                "category":      random.choice(["salary","procurement","grants","capex","opex"]),
                "district":      random.choice(DISTRICTS),
                "txn_ref":       f"TXN-LIVE-{random.randint(10000,99999)}",
                "is_flagged":    random.random() < 0.08,
            }
        elif etype == "anomaly":
            sev = random.choices(["critical","high","medium","low"], weights=[10,25,40,25])[0]
            return {
                "event_type":    "anomaly",
                "topic":         "budget.anomalies",
                "timestamp":     now,
                "department_id": dept["id"],
                "dept_name":     dept["name"],
                "dept_color":    dept["color"],
                "severity":      sev,
                "anomaly_type":  random.choice(["spending_spike","vendor_concentration",
                                                "duplicate_payment","ghost_beneficiary"]),
                "title":         f"Live Alert — {dept['name']}",
                "amount_cr":     round(random.uniform(5.0, 400.0), 1),
                "ml_score":      round(random.uniform(55, 99), 1),
                "district":      random.choice(DISTRICTS),
                "status":        "open",
            }
        elif etype == "transfer":
            src, dst = random.sample(DEPTS, 2)
            return {
                "event_type": "transfer",
                "topic":      "budget.transfers",
                "timestamp":  now,
                "from_dept":  src["id"],
                "from_name":  src["name"],
                "to_dept":    dst["id"],
                "to_name":    dst["name"],
                "amount_cr":  round(random.uniform(50.0, 800.0), 1),
                "status":     random.choice(["pending","approved","completed"]),
                "reason":     "AI-recommended reallocation",
            }
        else:
            util = round(random.uniform(60.0, 68.0), 1)
            return {
                "event_type":          "kpi_update",
                "topic":               "budget.kpi_updates",
                "timestamp":           now,
                "overall_utilization": util,
                "total_spent_cr":      round(util / 100 * 412000, 0),
                "lapse_risk_cr":       round(random.uniform(2600, 3100), 0),
                "anomalies_open":      random.randint(42, 55),
                "critical_count":      random.randint(5, 10),
                "efficiency_score":    round(random.uniform(69.0, 74.0), 1),
            }

    def poll(self):
        """Return next event from file or synthetic generator."""
        # Try reading from file first
        if os.path.exists(self.filepath):
            try:
                with open(self.filepath, "r") as f:
                    lines = f.readlines()
                if self._pos < len(lines):
                    event = json.loads(lines[self._pos].strip())
                    self._pos += 1
                    return event
                # File exhausted — wrap around
                self._pos = 0
            except Exception:
                pass
        # Fall back to synthetic
        return self._synthetic_event()


# ── Real Kafka consumer ──

class KafkaConsumerWrapper:
    def __init__(self, broker: str, topics: list):
        try:
            from kafka import KafkaConsumer as _KC
            self._c = _KC(
                *topics,
                bootstrap_servers=broker,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                group_id="budgetflow-sse-bridge",
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                consumer_timeout_ms=1000,
            )
            print(f"[KafkaConsumer] Connected to {broker}, topics: {topics}")
        except ImportError:
            raise RuntimeError("kafka-python not installed. Run: pip install kafka-python")
        except Exception as e:
            raise RuntimeError(f"Cannot connect to Kafka broker {broker}: {e}")

    def poll(self):
        """Return next message or None if timeout."""
        try:
            for msg in self._c:
                return msg.value
        except Exception:
            return None

    def close(self):
        self._c.close()


# ── Consumer thread ──

def _consumer_loop(consumer, interval: float = 2.0):
    """Background thread: poll consumer → push to queue."""
    print("[ConsumerThread] Started — bridging events to SSE queue")
    while True:
        try:
            event = consumer.poll()
            if event:
                push_event(event)
                ts = datetime.now().strftime("%H:%M:%S")
                print(f"  [{ts}] ← {event.get('topic','?'):25s} | "
                      f"{event.get('event_type','?'):12s} | "
                      f"{event.get('dept_name','')[:20]}")
            time.sleep(interval)
        except Exception as e:
            print(f"[ConsumerThread] Error: {e}")
            time.sleep(5)


def start_consumer_thread(
    broker: str = "localhost:9092",
    mock: bool = True,
    interval: float = 2.0,
    mock_file: str = "mock_stream.jsonl",
) -> threading.Thread:
    """
    Start the Kafka consumer in a background daemon thread.
    Called once from main.py on startup.

    Args:
        broker:    Kafka broker address
        mock:      If True, use MockConsumer (no Kafka needed)
        interval:  Seconds between polls
        mock_file: Path to mock_stream.jsonl
    """
    global _CONSUMER_STARTED
    if _CONSUMER_STARTED:
        return None
    _CONSUMER_STARTED = True

    if mock:
        consumer = MockConsumer(filepath=mock_file, interval=interval)
    else:
        try:
            consumer = KafkaConsumerWrapper(
                broker=broker,
                topics=[
                    "budget.expenditures",
                    "budget.anomalies",
                    "budget.transfers",
                    "budget.kpi_updates",
                ]
            )
        except RuntimeError as e:
            print(f"[Consumer] Kafka unavailable ({e}), falling back to MockConsumer")
            consumer = MockConsumer(filepath=mock_file, interval=interval)

    t = threading.Thread(
        target=_consumer_loop,
        args=(consumer, interval),
        daemon=True,
        name="KafkaConsumerThread"
    )
    t.start()
    return t


# ── Standalone test ──

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BudgetFlow Kafka Consumer")
    parser.add_argument("--broker",   default="localhost:9092")
    parser.add_argument("--interval", type=float, default=2.0)
    parser.add_argument("--mock",     action="store_true")
    parser.add_argument("--file",     default="mock_stream.jsonl")
    args = parser.parse_args()

    print("=" * 60)
    print("  BudgetFlow — Kafka Consumer (standalone test)")
    print("=" * 60)

    t = start_consumer_thread(
        broker=args.broker,
        mock=args.mock,
        interval=args.interval,
        mock_file=args.file,
    )

    print("Consuming events — press Ctrl+C to stop\n")
    try:
        while True:
            time.sleep(1)
            with _QUEUE_LOCK:
                print(f"  Queue size: {len(_EVENT_QUEUE)} events", end="\r")
    except KeyboardInterrupt:
        print("\nStopped.")
