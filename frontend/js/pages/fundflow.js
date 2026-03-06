/**
 * Fund Flow Page — Bézier-curve Sankey diagram of money trails
 */
Pages.fundFlow = async function(root) {
  root.innerHTML = "";

  const [flowRes, utilRes] = await Promise.all([
    Api.fundFlow({ fy: "FY2025" }),
    Api.utilization({ fy: "FY2025" }),
  ]);

  const { links } = flowRes.data;
  const { by_category } = utilRes.data;

  // Gather unique dept names (sources) as nodes
  const sources = [...new Set(links.map(l => l.source))];
  const targets = [...new Set(links.map(l => l.target))];

  const topLinks = links
    .sort((a, b) => b.value - a.value)
    .slice(0, 20);

  root.innerHTML = `
    <div class="page-header">
      <div>
        <h1 class="page-title">Fund Flow</h1>
        <p class="page-subtitle">Department → Scheme money trail — FY 2024-25</p>
      </div>
    </div>

    <div class="grid-2" style="margin-bottom:22px">
      <!-- Sankey -->
      <div class="card" style="grid-column:1/-1">
        <div class="card-title">Sankey Flow Diagram · Departments → Schemes (Top 20 Flows)</div>
        <div class="chart-canvas-wrap" style="height:420px">
          <canvas id="sankey-canvas" style="width:100%;height:420px"></canvas>
        </div>
        <div class="sankey-legend" id="sankey-legend"></div>
      </div>
    </div>

    <div class="grid-2" style="margin-bottom:22px">
      <!-- Category breakdown -->
      <div class="card">
        <div class="card-title">Utilization by Category</div>
        <div id="cat-bars">
          ${by_category.map(c => `
            <div style="margin-bottom:12px">
              <div style="display:flex;justify-content:space-between;margin-bottom:4px">
                <span style="font-size:12px;color:var(--text-secondary);text-transform:capitalize">${c.category}</span>
                <span style="font-size:12px;font-weight:700;color:var(--text-primary)">${fmtPct(c.pct)}</span>
              </div>
              <div class="progress-wrap">
                <div class="progress-fill ${utilColor(c.pct)}" style="width:${Math.min(100,c.pct||0)}%"></div>
              </div>
              <div style="display:flex;justify-content:space-between;margin-top:3px;font-size:11px;color:var(--text-muted)">
                <span>${fmtCr(c.utilized)} utilized</span>
                <span>${fmtCr(c.allocated)} allocated</span>
              </div>
            </div>
          `).join("")}
        </div>
      </div>

      <!-- Top 10 flows table -->
      <div class="card">
        <div class="card-title">Top Fund Flows</div>
        <div class="table-wrap">
          <table>
            <thead><tr>
              <th>Department</th>
              <th>Scheme</th>
              <th>Utilized</th>
            </tr></thead>
            <tbody>
              ${topLinks.slice(0, 10).map((l, i) => `
                <tr>
                  <td class="strong">${l.source}</td>
                  <td style="color:var(--text-secondary)">${l.target.length > 24 ? l.target.slice(0,23)+"…" : l.target}</td>
                  <td style="color:var(--green);font-weight:700">${fmtCr(l.value)}</td>
                </tr>
              `).join("")}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  `;

  // Draw Sankey
  const canvas = document.getElementById("sankey-canvas");
  if (canvas && topLinks.length) {
    canvas.style.cssText = "width:100%;height:420px";
    requestAnimationFrame(() => {
      Charts.drawSankey(canvas, null, topLinks);
    });
  }

  // Legend
  const legend = document.getElementById("sankey-legend");
  if (legend) {
    sources.slice(0, 8).forEach((s, i) => {
      const dot = `<div class="sankey-legend-dot" style="background:${Charts.PALETTE[i % Charts.PALETTE.length]}"></div>`;
      legend.innerHTML += `<div class="sankey-legend-item">${dot}${s}</div>`;
    });
  }
};
