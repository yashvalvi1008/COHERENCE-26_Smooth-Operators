/**
 * Anomaly Log Page — Categorized fraud/waste risk items
 */
Pages.anomalies = async function(root) {
  root.innerHTML = "";

  const res = await Api.anomalies({ status: "open" });
  const items = res.data;

  const counts = { critical: 0, high: 0, medium: 0 };
  items.forEach(a => { if (counts[a.severity] !== undefined) counts[a.severity]++; });

  root.innerHTML = `
    <div class="page-header">
      <div>
        <h1 class="page-title">Anomaly Log</h1>
        <p class="page-subtitle">${items.length} open anomalies detected — FY 2024-25</p>
      </div>
      <div style="display:flex;gap:10px">
        <select id="filter-severity" class="filter-bar">
          <option value="">All Severities</option>
          <option value="critical">Critical</option>
          <option value="high">High</option>
          <option value="medium">Medium</option>
        </select>
        <select id="filter-type" class="filter-bar">
          <option value="">All Types</option>
          <option value="z_score_outlier">Z-Score Spike</option>
          <option value="iqr_outlier">IQR Outlier</option>
          <option value="benford_violation">Benford's Law</option>
          <option value="salami_slicing">Salami Slicing</option>
          <option value="year_end_rush">Year-End Rush</option>
        </select>
      </div>
    </div>

    <!-- Summary strip -->
    <div class="grid-4" style="margin-bottom:22px">
      ${_anomStat("Total Open", items.length, "var(--text-secondary)")}
      ${_anomStat("Critical", counts.critical, "var(--red)")}
      ${_anomStat("High", counts.high, "var(--orange)")}
      ${_anomStat("Medium", counts.medium, "var(--yellow)")}
    </div>

    <!-- Type breakdown -->
    <div class="grid-2" style="margin-bottom:22px">
      <div class="card">
        <div class="card-title">Anomaly Distribution</div>
        <canvas id="anom-donut" style="display:block;margin:auto;width:180px;height:180px"></canvas>
      </div>
      <div class="card">
        <div class="card-title">By Type Count</div>
        <canvas id="anom-type-bar" style="width:100%;height:200px"></canvas>
      </div>
    </div>

    <!-- Full list -->
    <div class="card">
      <div class="card-title">Anomaly Details</div>
      <div id="anom-list">
        ${_renderList(items)}
      </div>
    </div>
  `;

  // Donut
  const donut = document.getElementById("anom-donut");
  if (donut) {
    const typeCounts = {};
    items.forEach(a => { typeCounts[a.anomaly_type] = (typeCounts[a.anomaly_type] || 0) + 1; });
    Charts.drawDonut(donut, Object.entries(typeCounts).map(([k, v]) => ({
      label: anomalyTypeLabel(k), value: v,
    })), { centerLabel: `${items.length}` });
  }

  // Type bar
  const typeBar = document.getElementById("anom-type-bar");
  if (typeBar) {
    const typeCounts2 = {};
    items.forEach(a => { typeCounts2[anomalyTypeLabel(a.anomaly_type)] = (typeCounts2[anomalyTypeLabel(a.anomaly_type)] || 0) + 1; });
    const barData = Object.entries(typeCounts2)
      .sort((a, b) => b[1] - a[1])
      .map(([k, v]) => ({ label: k, value: v }));
    typeBar.style.cssText = `width:100%;height:${barData.length * 32 + 20}px`;
    Charts.drawBarChart(typeBar, barData, { color: "#a855f7", unit: "" });
  }

  // Filter handlers
  const applyFilter = async () => {
    const sev  = document.getElementById("filter-severity")?.value;
    const type = document.getElementById("filter-type")?.value;
    const fRes = await Api.anomalies({ status: "open", ...(sev ? { severity: sev } : {}) });
    let filtered = fRes.data;
    if (type) filtered = filtered.filter(a => a.anomaly_type === type);
    const list = document.getElementById("anom-list");
    if (list) list.innerHTML = _renderList(filtered);
  };

  document.getElementById("filter-severity")?.addEventListener("change", applyFilter);
  document.getElementById("filter-type")?.addEventListener("change", applyFilter);
};

function _anomStat(label, count, color) {
  return `
    <div class="kpi-card">
      <div class="kpi-label">${label}</div>
      <div class="kpi-value" style="color:${color}">${count}</div>
    </div>`;
}

function _renderList(items) {
  if (!items.length) return `<div class="empty-state"><div class="empty-state-icon">✓</div><p>No anomalies found</p></div>`;
  return items.map(a => `
    <div class="anom-item ${a.severity}">
      <div class="anom-score-ring ${a.severity}">${Math.round(a.score)}</div>
      <div class="anom-body">
        <div class="anom-title">
          ${anomalyTypeLabel(a.anomaly_type)}
          &nbsp;${severityBadge(a.severity)}
          ${a.txn_id ? `<span style="font-size:11px;color:var(--text-muted);margin-left:6px">${a.txn_id}</span>` : ""}
        </div>
        <div class="anom-desc">${a.description}</div>
        <div class="anom-meta">
          <strong>${a.scheme_name}</strong> · ${a.dept_name}
          · Detected: ${a.detected_at ? a.detected_at.slice(0, 16).replace("T", " ") : "—"}
        </div>
      </div>
      <div style="flex-shrink:0">
        <button class="btn btn-ghost btn-sm" onclick="App.toast('Anomaly ${a.id} marked for review','info')">Review</button>
      </div>
    </div>
  `).join("");
}
