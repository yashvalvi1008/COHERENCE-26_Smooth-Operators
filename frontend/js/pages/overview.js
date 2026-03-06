/**
 * Overview Page — KPI dashboard, quarterly chart, top departments
 */
Pages = window.Pages || {};

Pages.overview = async function(root) {
  root.innerHTML = "";

  const res = await Api.overview({ fy: "FY2025" });
  const { kpis, quarterly_utilization, top_departments, recent_anomalies } = res.data;

  const html = `
    <div class="page-header">
      <div>
        <h1 class="page-title">Budget Overview</h1>
        <p class="page-subtitle">Public fund lifecycle summary — FY 2024-25</p>
      </div>
    </div>

    <!-- KPI Cards -->
    <div class="grid-4" style="margin-bottom:22px">
      ${_kpiCard("Total Allocation", fmtCr(kpis.total_allocation), "Across all active schemes", "blue")}
      ${_kpiCard("Total Utilized", fmtCr(kpis.total_utilized), `${fmtPct(kpis.utilization_pct)} utilization rate`, utilColor(kpis.utilization_pct))}
      ${_kpiCard("Active Anomalies", kpis.active_anomalies, `${kpis.critical_anomalies} critical`, kpis.active_anomalies > 0 ? "red" : "green")}
      ${_kpiCard("Active Schemes", kpis.active_schemes, `${kpis.active_departments} departments`, "purple")}
    </div>

    <!-- Utilization Gauge + Quarterly -->
    <div class="grid-2" style="margin-bottom:22px">
      <div class="card">
        <div class="card-title">Utilization Rate</div>
        <div style="display:flex;align-items:center;gap:20px">
          <div style="flex:1">
            <div style="font-size:48px;font-weight:900;color:var(--text-primary);line-height:1">${fmtPct(kpis.utilization_pct)}</div>
            <div style="font-size:13px;color:var(--text-muted);margin-top:6px">${fmtCr(kpis.total_utilized)} of ${fmtCr(kpis.total_allocation)} allocated</div>
            <div class="progress-wrap" style="margin-top:14px;height:10px">
              <div class="progress-fill ${utilColor(kpis.utilization_pct)}" style="width:${Math.min(100,kpis.utilization_pct)}%"></div>
            </div>
          </div>
          <canvas id="donut-util" style="width:110px;height:110px"></canvas>
        </div>
      </div>
      <div class="card">
        <div class="card-title">Quarterly Utilization (FY25)</div>
        <canvas id="chart-quarterly" style="width:100%;height:160px"></canvas>
      </div>
    </div>

    <!-- Top departments + Recent anomalies -->
    <div class="grid-2" style="margin-bottom:22px">
      <div class="card">
        <div class="card-title">Department Performance</div>
        <canvas id="chart-depts" style="width:100%;height:${top_departments.length * 32 + 20}px"></canvas>
      </div>
      <div class="card">
        <div class="card-title">Recent Anomalies</div>
        <div id="recent-anom-list">${_recentAnomList(recent_anomalies)}</div>
      </div>
    </div>
  `;

  root.innerHTML = html;

  // Donut
  const donutCanvas = document.getElementById("donut-util");
  if (donutCanvas) {
    donutCanvas.style.cssText = "width:110px;height:110px";
    const util = kpis.utilization_pct;
    Charts.drawDonut(donutCanvas, [
      { label: "Utilized", value: util },
      { label: "Remaining", value: Math.max(0, 100 - util) },
    ], { centerLabel: fmtPct(util) });
  }

  // Quarterly bar
  const qCanvas = document.getElementById("chart-quarterly");
  if (qCanvas && quarterly_utilization.length) {
    qCanvas.style.cssText = "width:100%;height:160px";
    Charts.drawForecast(qCanvas, quarterly_utilization.map(q => ({
      label: `Q${q.quarter}`,
      allocated: q.allocated,
      utilized:  q.utilized,
    })));
  }

  // Dept horizontal bar
  const dCanvas = document.getElementById("chart-depts");
  if (dCanvas && top_departments.length) {
    dCanvas.style.cssText = `width:100%;height:${top_departments.length * 32 + 20}px`;
    Charts.drawBarChart(dCanvas, top_departments.map(d => ({
      label: d.short_name,
      value: d.utilization_pct,
    })), { valueKey:"value", labelKey:"label", color:"#3b82f6", unit:"%" });
  }
};

function _kpiCard(label, value, sub, color = "blue") {
  return `
    <div class="kpi-card ${color}">
      <div class="kpi-label">${label}</div>
      <div class="kpi-value">${value}</div>
      <div class="kpi-sub">${sub}</div>
    </div>`;
}

function _recentAnomList(anoms) {
  if (!anoms || !anoms.length) return `<div class="empty-state" style="padding:30px">No open anomalies</div>`;
  return anoms.map(a => `
    <div class="anom-item ${a.severity}" style="margin-bottom:8px;padding:10px 12px">
      <div class="anom-score-ring ${a.severity}">${Math.round(a.score)}</div>
      <div class="anom-body">
        <div class="anom-title">${anomalyTypeLabel(a.anomaly_type)} ${severityBadge(a.severity)}</div>
        <div class="anom-meta">${a.scheme_name} · ${a.dept_name}</div>
      </div>
    </div>
  `).join("");
}
