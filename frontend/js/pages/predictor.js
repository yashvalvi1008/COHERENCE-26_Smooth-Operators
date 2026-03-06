/**
 * Predictive Risk Page — Forecast chart + lapse probability risk matrix
 */
Pages.predictor = async function(root) {
  root.innerHTML = "";

  const [forecastRes, riskRes] = await Promise.all([
    Api.forecast({ fy: "FY2025" }),
    Api.riskMatrix({ fy: "FY2025" }),
  ]);

  const forecasts = forecastRes.data;
  const riskScores = riskRes.data;

  // Summary counts
  const lapseCounts = { critical: 0, high: 0, medium: 0, low: 0 };
  forecasts.forEach(f => lapseCounts[f.risk_level] = (lapseCounts[f.risk_level] || 0) + 1);

  root.innerHTML = `
    <div class="page-header">
      <div>
        <h1 class="page-title">Predictive Risk</h1>
        <p class="page-subtitle">OLS + Exponential Smoothing (α=0.35) — lapse prevention analytics</p>
      </div>
    </div>

    <!-- Risk summary strip -->
    <div class="grid-4" style="margin-bottom:22px">
      ${_riskStat("Critical Lapse Risk",  lapseCounts.critical, "var(--red)")}
      ${_riskStat("High Lapse Risk",      lapseCounts.high,     "var(--orange)")}
      ${_riskStat("Medium Lapse Risk",    lapseCounts.medium,   "var(--yellow)")}
      ${_riskStat("Low / On-Track",       lapseCounts.low,      "var(--green)")}
    </div>

    <!-- Forecast rows + chart -->
    <div class="grid-2" style="margin-bottom:22px">
      <div class="card" style="grid-column:1/-1">
        <div class="card-title">Department Risk Matrix — Predicted Utilization vs. Lapse Probability</div>
        <div id="forecast-rows">
          ${_renderForecastRows(forecasts)}
        </div>
      </div>
    </div>

    <!-- Composite risk scores table -->
    <div class="card">
      <div class="card-title">Composite Risk Scores (7-Signal Model)</div>
      <div class="table-wrap">
        <table>
          <thead><tr>
            <th>Scheme</th>
            <th>Department</th>
            <th>Risk Score</th>
            <th>Z-Score</th>
            <th>Benford</th>
            <th>Salami</th>
            <th>Year-End</th>
            <th>Lapse Prob</th>
          </tr></thead>
          <tbody>
            ${riskScores.slice(0, 20).map(r => `
              <tr>
                <td class="strong">${r.scheme_name}</td>
                <td style="color:var(--text-muted)">${r.short_name}</td>
                <td>
                  <div style="display:flex;align-items:center;gap:8px">
                    <div style="width:60px;background:var(--bg-elevated);border-radius:3px;height:5px;overflow:hidden">
                      <div style="width:${r.composite_score}%;height:100%;background:${_scoreColor(r.composite_score)};border-radius:3px"></div>
                    </div>
                    <span style="font-weight:700;color:${_scoreColor(r.composite_score)}">${r.composite_score}</span>
                  </div>
                </td>
                <td>${_flag(r.zscore_flag)}</td>
                <td>${_flag(r.benford_flag)}</td>
                <td>${_flag(r.salami_flag)}</td>
                <td>${_flag(r.yearend_flag)}</td>
                <td style="font-weight:700;color:${_lapseColor(r.lapse_probability)}">${(r.lapse_probability * 100).toFixed(0)}%</td>
              </tr>
            `).join("")}
          </tbody>
        </table>
      </div>
    </div>
  `;
};

function _riskStat(label, count, color) {
  return `
    <div class="kpi-card">
      <div class="kpi-label">${label}</div>
      <div class="kpi-value" style="color:${color}">${count}</div>
      <div class="kpi-sub">Departments</div>
    </div>`;
}

function _renderForecastRows(forecasts) {
  return forecasts.map(f => {
    const pct  = f.predicted_utilization_pct || 0;
    const actPct = f.total_allocated > 0 ? (f.total_utilized / f.total_allocated * 100) : 0;
    const lp   = f.lapse_probability;
    const trendClass = `trend-${f.trend}`;
    const trendArrow = f.trend === "increasing" ? "↑" : f.trend === "decreasing" ? "↓" : "→";

    return `
      <div class="forecast-row">
        <div class="forecast-dept">
          <div class="forecast-dept-name">${f.dept_name}</div>
          <div class="forecast-dept-short">${f.short_name}</div>
        </div>
        <div class="forecast-bar-wrap">
          <div style="display:flex;justify-content:space-between;margin-bottom:4px;font-size:11px;color:var(--text-muted)">
            <span>Actual ${fmtPct(actPct)}</span>
            <span>Forecast ${fmtPct(pct)}</span>
          </div>
          <div class="forecast-bar-bg" style="position:relative">
            <div class="forecast-bar-fill-actual" style="width:${Math.min(100,actPct)}%;position:absolute;top:0;left:0;height:100%"></div>
            <div class="forecast-bar-fill-project" style="width:${Math.min(100,pct)}%;height:100%;opacity:0.6"></div>
          </div>
        </div>
        <div class="forecast-nums">
          <div class="forecast-pct">${fmtPct(pct)}</div>
          <div style="font-size:11px;margin-top:2px">
            <span class="forecast-trend-chip ${trendClass}">${trendArrow} ${f.trend}</span>
          </div>
        </div>
        <div style="width:80px;text-align:right">
          <div style="font-size:13px;font-weight:700;color:${_lapseColor(lp)}">${(lp*100).toFixed(0)}%</div>
          <div style="font-size:10px;color:var(--text-muted)">Lapse Prob</div>
          <span class="badge badge-${f.risk_level}">${f.risk_level}</span>
        </div>
      </div>`;
  }).join("");
}

function _scoreColor(score) {
  if (score >= 70) return "var(--red)";
  if (score >= 45) return "var(--orange)";
  if (score >= 25) return "var(--yellow)";
  return "var(--green)";
}

function _lapseColor(prob) {
  if (prob >= 0.6) return "var(--red)";
  if (prob >= 0.4) return "var(--orange)";
  if (prob >= 0.2) return "var(--yellow)";
  return "var(--green)";
}

function _flag(val) {
  return val
    ? `<span style="color:var(--red);font-weight:700">⚑ Yes</span>`
    : `<span style="color:var(--text-muted)">—</span>`;
}
