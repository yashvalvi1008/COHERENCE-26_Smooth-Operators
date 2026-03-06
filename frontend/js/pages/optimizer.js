/**
 * Optimizer Page — Greedy reallocation recommendations
 */
Pages.optimizer = async function(root) {
  root.innerHTML = "";

  const res = await Api.optimizer({ fy: "FY2025" });
  const recs = res.data;

  const highCount = recs.filter(r => r.priority === "high").length;
  const totalAmount = recs.reduce((s, r) => s + r.transfer_amount, 0);
  const applied = { count: 0 };

  root.innerHTML = `
    <div class="page-header">
      <div>
        <h1 class="page-title">Reallocation Optimizer</h1>
        <p class="page-subtitle">Greedy algorithm — surplus → deficit fund matching</p>
      </div>
    </div>

    <!-- Stats -->
    <div class="grid-4" style="margin-bottom:22px">
      <div class="kpi-card blue">
        <div class="kpi-label">Recommendations</div>
        <div class="kpi-value">${recs.length}</div>
        <div class="kpi-sub">${highCount} high priority</div>
      </div>
      <div class="kpi-card green">
        <div class="kpi-label">Total Realloc. Value</div>
        <div class="kpi-value" style="font-size:22px">${fmtCr(totalAmount)}</div>
        <div class="kpi-sub">Across all recommendations</div>
      </div>
      <div class="kpi-card yellow">
        <div class="kpi-label">High Priority</div>
        <div class="kpi-value">${highCount}</div>
        <div class="kpi-sub">Require immediate action</div>
      </div>
      <div class="kpi-card purple">
        <div class="kpi-label">Applied This Session</div>
        <div class="kpi-value" id="applied-count">0</div>
        <div class="kpi-sub">Click "Apply" to reallocate</div>
      </div>
    </div>

    <!-- Recommendations -->
    <div id="opt-list">
      ${recs.length === 0
        ? `<div class="empty-state" style="padding:60px"><div class="empty-state-icon">✓</div><p>No reallocation needed — all schemes are balanced.</p></div>`
        : recs.map(r => _recCard(r)).join("")
      }
    </div>
  `;

  // Apply button handlers
  document.querySelectorAll(".btn-apply-rec").forEach(btn => {
    btn.addEventListener("click", async () => {
      const recId = btn.dataset.rec;
      btn.disabled = true;
      btn.textContent = "Applying…";
      try {
        await Api.optimizerApply({ recommendation_id: recId });
        App.toast(`Recommendation ${recId} applied successfully`, "success");
        const card = document.getElementById(`rec-card-${recId}`);
        if (card) {
          card.style.opacity = "0.5";
          card.style.pointerEvents = "none";
          card.querySelector(".opt-actions").innerHTML =
            `<span class="badge badge-teal">✓ Applied</span>`;
        }
        applied.count++;
        const counter = document.getElementById("applied-count");
        if (counter) counter.textContent = applied.count;
      } catch (err) {
        App.toast(`Failed: ${err.message}`, "error");
        btn.disabled = false;
        btn.textContent = "Apply";
      }
    });
  });
};

function _recCard(r) {
  const sameCat = r.same_category
    ? `<span class="badge badge-teal" style="font-size:10px">✓ Same category</span>`
    : `<span class="badge badge-blue" style="font-size:10px">Cross-category</span>`;

  return `
    <div class="opt-card ${r.priority === 'high' ? 'high-priority' : ''}" id="rec-card-${r.id}">
      <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:12px">
        <div>
          <span class="badge badge-${r.priority === 'high' ? 'critical' : 'medium'}" style="margin-right:6px">${r.priority} priority</span>
          ${sameCat}
          <span style="font-size:11px;color:var(--text-muted);margin-left:6px">${r.id}</span>
        </div>
        <div class="opt-amount">${fmtCr(r.transfer_amount)}</div>
      </div>

      <div class="opt-flow">
        <!-- From -->
        <div class="opt-scheme-box" style="border-left:3px solid var(--red)">
          <div style="font-size:10px;color:var(--text-muted);font-weight:700;text-transform:uppercase;margin-bottom:4px">From (Surplus)</div>
          <div class="opt-scheme-name">${r.from_scheme_name}</div>
          <div class="opt-scheme-dept">${r.from_dept_name}</div>
          <div class="opt-util-pct" style="color:var(--red)">⬤ ${r.from_util_pct}% utilized</div>
        </div>

        <div class="opt-arrow">→</div>

        <!-- To -->
        <div class="opt-scheme-box" style="border-left:3px solid var(--green)">
          <div style="font-size:10px;color:var(--text-muted);font-weight:700;text-transform:uppercase;margin-bottom:4px">To (Deficit)</div>
          <div class="opt-scheme-name">${r.to_scheme_name}</div>
          <div class="opt-scheme-dept">${r.to_dept_name}</div>
          <div class="opt-util-pct" style="color:var(--green)">⬤ ${r.to_util_pct}% utilized</div>
        </div>
      </div>

      <div class="opt-reason">${r.reason}</div>

      <div class="opt-actions">
        <button class="btn btn-success btn-apply-rec" data-rec="${r.id}">✓ Apply Reallocation</button>
        <button class="btn btn-ghost btn-sm" onclick="App.toast('Detailed analysis coming soon','info')">View Details</button>
      </div>
    </div>
  `;
}
