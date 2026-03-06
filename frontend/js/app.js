/**
 * BudgetFlow Intelligence — SPA Router & App Shell
 */
const App = (() => {
  const PAGES = {
    "overview":   { title: "Overview",         render: Pages.overview  },
    "fund-flow":  { title: "Fund Flow",         render: Pages.fundFlow  },
    "anomalies":  { title: "Anomaly Log",       render: Pages.anomalies },
    "predictor":  { title: "Predictive Risk",   render: Pages.predictor },
    "optimizer":  { title: "Optimizer",         render: Pages.optimizer },
    "districts":  { title: "Districts",         render: Pages.districts },
  };

  let _currentPage = null;

  // ── Navigation ────────────────────────────────────────────────────────────
  function navigate(page) {
    if (!PAGES[page]) page = "overview";
    if (_currentPage === page) return;
    _currentPage = page;

    // Update nav
    document.querySelectorAll(".nav-item").forEach(el => {
      el.classList.toggle("active", el.dataset.page === page);
    });

    // Update breadcrumb
    const bc = document.getElementById("topbar-breadcrumb");
    if (bc) bc.textContent = PAGES[page].title;

    // Render
    const root = document.getElementById("page-root");
    root.innerHTML = "";
    const loader = document.createElement("div");
    loader.className = "page-loader";
    loader.innerHTML = `<div class="spinner"></div><p>Loading…</p>`;
    root.appendChild(loader);

    Promise.resolve()
      .then(() => PAGES[page].render(root))
      .catch(err => {
        root.innerHTML = `<div class="empty-state">
          <div class="empty-state-icon">⚠</div>
          <p>Failed to load page: ${err.message}</p>
        </div>`;
      });
  }

  // ── Toast ─────────────────────────────────────────────────────────────────
  function toast(message, type = "info") {
    const container = document.getElementById("toast-container");
    if (!container) return;
    const el = document.createElement("div");
    el.className = `toast ${type}`;
    el.textContent = message;
    container.appendChild(el);
    setTimeout(() => el.remove(), 4500);
  }

  // ── Scan button ───────────────────────────────────────────────────────────
  function _bindScan() {
    const btn = document.getElementById("btn-scan");
    if (!btn) return;
    btn.addEventListener("click", async () => {
      if (btn.classList.contains("scanning")) return;
      btn.classList.add("scanning");
      btn.textContent = "⟳ Scanning…";
      try {
        const res = await Api.scan({ fiscal_year: "FY2025" });
        const d = res.data;
        toast(`Scan complete — ${d.anomalies_detected} anomalies (${d.critical} critical, ${d.high} high)`, "success");
        _updateAnomalyBadge();
        const li = document.getElementById("last-scan-info");
        if (li) li.textContent = `Last scan: just now`;
        // Re-render current page to reflect new data
        const cp = _currentPage;
        _currentPage = null;
        navigate(cp);
      } catch (err) {
        toast(`Scan failed: ${err.message}`, "error");
      } finally {
        btn.classList.remove("scanning");
        btn.textContent = "⟳ Run Scan";
      }
    });
  }

  // ── Update anomaly badge ──────────────────────────────────────────────────
  async function _updateAnomalyBadge() {
    try {
      const res = await Api.anomalies({ status: "open" });
      const cnt = res.data.length;
      const badge = document.getElementById("nav-badge-anomaly");
      if (badge) badge.textContent = cnt > 0 ? cnt : "–";
      const dot = document.getElementById("topbar-alert-dot");
      if (dot) dot.classList.toggle("hidden", cnt === 0);
    } catch (_) {}
  }

  // ── Hash routing ──────────────────────────────────────────────────────────
  function _bindRouter() {
    const resolve = () => {
      const hash = location.hash.slice(1) || "overview";
      navigate(hash);
    };
    window.addEventListener("hashchange", resolve);
    resolve();
  }

  // ── Init ──────────────────────────────────────────────────────────────────
  function init() {
    // Nav click bindings
    document.querySelectorAll(".nav-item[data-page]").forEach(el => {
      el.addEventListener("click", e => {
        e.preventDefault();
        location.hash = el.dataset.page;
      });
    });

    _bindScan();
    _bindRouter();
    _updateAnomalyBadge();
  }

  document.addEventListener("DOMContentLoaded", init);

  return { navigate, toast };
})();

// ── Helpers exposed globally ──────────────────────────────────────────────────
function fmtCr(v) {
  if (!v && v !== 0) return "—";
  v = parseFloat(v);
  if (v >= 1e5) return "₹" + (v / 1e5).toFixed(2) + "L Cr";
  if (v >= 1e3) return "₹" + (v / 1e3).toFixed(1) + "K Cr";
  return "₹" + Math.round(v).toLocaleString() + " Cr";
}

function fmtPct(v) {
  if (v === null || v === undefined) return "—";
  return parseFloat(v).toFixed(1) + "%";
}

function severityBadge(sev) {
  return `<span class="badge badge-${sev}">${sev}</span>`;
}

function anomalyTypeLabel(t) {
  const map = {
    z_score_outlier:  "Z-Score Spike",
    iqr_outlier:      "IQR Outlier",
    benford_violation:"Benford's Law",
    salami_slicing:   "Salami Slicing",
    year_end_rush:    "Year-End Rush",
  };
  return map[t] || t.replace(/_/g, " ");
}

function utilColor(pct) {
  if (pct >= 85) return "green";
  if (pct >= 60) return "yellow";
  return "red";
}
