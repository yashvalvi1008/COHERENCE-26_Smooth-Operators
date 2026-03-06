/**
 * BudgetFlow Intelligence — API Client
 * Zero-dependency fetch wrapper for all 18 REST endpoints.
 */
const API_BASE = "http://localhost:8000/api";

const Api = (() => {
  async function _fetch(path, opts = {}) {
    const url = API_BASE + path;
    try {
      const res = await fetch(url, {
        headers: { "Content-Type": "application/json" },
        ...opts,
      });
      const json = await res.json();
      if (!res.ok) throw new Error(json.message || `HTTP ${res.status}`);
      return json;
    } catch (err) {
      App && App.toast(`API error: ${err.message}`, "error");
      throw err;
    }
  }

  function _qs(params = {}) {
    const q = Object.entries(params)
      .filter(([, v]) => v !== null && v !== undefined)
      .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v)}`)
      .join("&");
    return q ? "?" + q : "";
  }

  return {
    overview:         (p = {}) => _fetch("/overview"             + _qs(p)),
    departments:      (p = {}) => _fetch("/departments"          + _qs(p)),
    schemes:          (p = {}) => _fetch("/schemes"              + _qs(p)),
    districts:        (p = {}) => _fetch("/districts"            + _qs(p)),
    transactions:     (p = {}) => _fetch("/transactions"         + _qs(p)),
    anomalies:        (p = {}) => _fetch("/anomalies"            + _qs(p)),
    anomalyDetail:    (id)     => _fetch(`/anomalies/${id}`),
    riskMatrix:       (p = {}) => _fetch("/risk-matrix"          + _qs(p)),
    forecast:         (p = {}) => _fetch("/forecast"             + _qs(p)),
    optimizer:        (p = {}) => _fetch("/optimizer"            + _qs(p)),
    fundFlow:         (p = {}) => _fetch("/fund-flow"            + _qs(p)),
    districtsPerf:    (p = {}) => _fetch("/districts/performance"+ _qs(p)),
    auditLog:         (p = {}) => _fetch("/audit-log"            + _qs(p)),
    alerts:           ()       => _fetch("/alerts"),
    utilization:      (p = {}) => _fetch("/utilization"         + _qs(p)),
    benchmarks:       (p = {}) => _fetch("/benchmarks"          + _qs(p)),
    scan:             (body)   => _fetch("/scan",           { method: "POST", body: JSON.stringify(body) }),
    optimizerApply:   (body)   => _fetch("/optimizer/apply",{ method: "POST", body: JSON.stringify(body) }),
  };
})();
