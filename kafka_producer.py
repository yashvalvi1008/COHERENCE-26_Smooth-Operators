"""
BudgetFlow Intelligence — Kafka Producer
Simulates real-time budget events streaming to Kafka topics.

Topics produced:
  - budget.expenditures   : live spend transactions
  - budget.anomalies      : newly detected anomalies
  - budget.transfers      : fund transfer events
  - budget.kpi_updates    : KPI snapshots every 30s

Usage:
    pip install kafka-python
    python kafka_producer.py                        # default localhost:9092
    python kafka_producer.py --broker localhost:9092 --interval 3
    python kafka_producer.py --mock                 # no Kafka needed (writes to mock_stream.jsonl)
"""

import json
import random
import time
import argparse
import threading
from datetime import datetime, timedelta

# ── department & district pools ──
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
DISTRICTS = ["Mumbai", "Lucknow", "Chennai", "Patna", "Bengaluru",
             "Jaipur", "Bhopal", "Hyderabad", "Kolkata", "Ahmedabad"]
VENDORS   = [f"VND{i:04d}" for i in range(1000, 1050)]
ANOMALY_TYPES = [
    "spending_spike", "vendor_concentration", "duplicate_payment",
    "ghost_beneficiary", "fund_stall", "year_end_rush", "procurement_split"
]
CATEGORIES = ["salary", "procurement", "grants", "capex", "opex"]

_seq = 0
def _next_seq():
    global _seq
    _seq += 1
    return _seq


# ── event generators ──

def gen_expenditure():
    dept = random.choice(DEPTS)
    amount = round(random.uniform(0.5, 120.0), 2)
    return {
        "event_type":    "expenditure",
        "topic":         "budget.expenditures",
        "seq":           _next_seq(),
        "timestamp":     datetime.utcnow().isoformat() + "Z",
        "department_id": dept["id"],
        "dept_name":     dept["name"],
        "dept_color":    dept["color"],
        "amount_cr":     amount,
        "category":      random.choice(CATEGORIES),
        "vendor_id":     random.choice(VENDORS) if random.random() > 0.4 else None,
        "district":      random.choice(DISTRICTS),
        "txn_ref":       f"TXN-{dept['id'].upper()}-{_seq:06d}",
        "fiscal_year":   "2024-25",
        "is_flagged":    random.random() < 0.08,   # 8% flagged
    }


def gen_anomaly():
    dept   = random.choice(DEPTS)
    sev    = random.choices(
        ["critical", "high", "medium", "low"],
        weights=[10, 25, 40, 25]
    )[0]
    amount = round(random.uniform(5.0, 400.0), 1)
    atype  = random.choice(ANOMALY_TYPES)
    titles = {
        "spending_spike":       f"Unusual Spending Spike — {dept['name']}",
        "vendor_concentration": f"Vendor Concentration Alert — {dept['name']}",
        "duplicate_payment":    f"Duplicate Payment Detected — {dept['name']}",
        "ghost_beneficiary":    f"Ghost Beneficiary Pattern — {dept['name']}",
        "fund_stall":           f"Stalled Funds Warning — {dept['name']}",
        "year_end_rush":        f"Year-End Rush Pattern — {dept['name']}",
        "procurement_split":    f"Contract Splitting Alert — {dept['name']}",
    }
    ml_score = round(random.uniform(55, 99), 1)
    return {
        "event_type":    "anomaly",
        "topic":         "budget.anomalies",
        "seq":           _next_seq(),
        "timestamp":     datetime.utcnow().isoformat() + "Z",
        "department_id": dept["id"],
        "dept_name":     dept["name"],
        "dept_color":    dept["color"],
        "severity":      sev,
        "anomaly_type":  atype,
        "title":         titles[atype],
        "amount_cr":     amount,
        "ml_score":      ml_score,
        "district":      random.choice(DISTRICTS),
        "status":        "open",
    }


def gen_transfer():
    src, dst = random.sample(DEPTS, 2)
    amount   = round(random.uniform(50.0, 800.0), 1)
    statuses = ["pending", "approved", "completed"]
    return {
        "event_type":  "transfer",
        "topic":       "budget.transfers",
        "seq":         _next_seq(),
        "timestamp":   datetime.utcnow().isoformat() + "Z",
        "from_dept":   src["id"],
        "from_name":   src["name"],
        "to_dept":     dst["id"],
        "to_name":     dst["name"],
        "amount_cr":   amount,
        "status":      random.choice(statuses),
        "reason":      random.choice([
            "Lapse prevention reallocation",
            "AI-recommended reallocation",
            "Emergency Q4 funding",
            "Scheme convergence",
            "Q3 savings transfer",
        ]),
        "fiscal_year": "2024-25",
    }


def gen_kpi_update():
    util = round(random.uniform(60.0, 68.0), 1)
    return {
        "event_type":          "kpi_update",
        "topic":               "budget.kpi_updates",
        "seq":                 _next_seq(),
        "timestamp":           datetime.utcnow().isoformat() + "Z",
        "overall_utilization": util,
        "total_spent_cr":      round(util / 100 * 412000, 0),
        "lapse_risk_cr":       round(random.uniform(2600, 3100), 0),
        "anomalies_open":      random.randint(42, 55),
        "critical_count":      random.randint(5, 10),
        "high_count":          random.randint(15, 22),
        "efficiency_score":    round(random.uniform(69.0, 74.0), 1),
    }


# ── producer classes ──

class MockProducer:
    """Writes events to a .jsonl file instead of Kafka (no broker needed)."""

    def __init__(self, filepath="mock_stream.jsonl"):
        self.filepath = filepath
        print(f"[MockProducer] Writing events to: {filepath}")

    def send(self, topic, value):
        event = json.loads(value.decode("utf-8"))
        with open(self.filepath, "a") as f:
            f.write(json.dumps(event) + "\n")

    def flush(self):
        pass

    def close(self):
        pass


class KafkaProducerWrapper:
    """Real Kafka producer using kafka-python."""

    def __init__(self, broker: str):
        try:
            from kafka import KafkaProducer as _KP
            self._p = _KP(
                bootstrap_servers=broker,
                value_serializer=lambda v: v,   # already bytes
                acks="all",
                retries=3,
            )
            print(f"[KafkaProducer] Connected to broker: {broker}")
        except ImportError:
            raise RuntimeError("kafka-python not installed. Run: pip install kafka-python")
        except Exception as e:
            raise RuntimeError(f"Cannot connect to Kafka broker {broker}: {e}")

    def send(self, topic, value):
        self._p.send(topic, value=value)

    def flush(self):
        self._p.flush()

    def close(self):
        self._p.close()


# ── main loop ──

def produce_events(producer, interval: float = 2.0, run_once: bool = False):
    """
    Continuously produce a mix of events.
    Weights: 60% expenditure, 20% anomaly, 10% transfer, 10% kpi_update
    """
    generators = [gen_expenditure] * 6 + [gen_anomaly] * 2 + [gen_transfer] + [gen_kpi_update]
    print(f"[Producer] Streaming events every {interval}s — Press Ctrl+C to stop")

    iteration = 0
    try:
        while True:
            gen = random.choice(generators)
            event = gen()
            payload = json.dumps(event, default=str).encode("utf-8")
            producer.send(event["topic"], value=payload)
            producer.flush()
            ts = datetime.now().strftime("%H:%M:%S")
            print(f"  [{ts}] → {event['topic']} | {event['event_type']:12s} | "
                  f"{event.get('dept_name','')[:22]:<22} | "
                  f"₹{event.get('amount_cr', event.get('overall_utilization',''))}")
            iteration += 1
            if run_once:
                break
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\n[Producer] Stopped by user.")
    finally:
        producer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BudgetFlow Kafka Producer")
    parser.add_argument("--broker",   default="localhost:9092",
                        help="Kafka broker address (default: localhost:9092)")
    parser.add_argument("--interval", type=float, default=2.0,
                        help="Seconds between events (default: 2.0)")
    parser.add_argument("--mock",     action="store_true",
                        help="Use mock mode — write to mock_stream.jsonl instead of Kafka")
    args = parser.parse_args()

    if args.mock:
        producer = MockProducer()
    else:
        producer = KafkaProducerWrapper(args.broker)

    produce_events(producer, interval=args.interval)
