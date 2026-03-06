/**
 * BudgetFlow Intelligence — Canvas Chart Library
 * Custom implementations: Bar, Donut, Sankey (Bézier), Forecast Line, Heatmap.
 */
const Charts = (() => {
  const PALETTE = [
    "#3b82f6","#22c55e","#f59e0b","#ef4444","#a855f7",
    "#14b8a6","#f97316","#ec4899","#64748b","#06b6d4",
    "#84cc16","#fb923c","#e879f9","#2dd4bf","#facc15",
  ];

  function _dpi(canvas) {
    const dpr = window.devicePixelRatio || 1;
    const rect = canvas.getBoundingClientRect();
    canvas.width  = rect.width  * dpr;
    canvas.height = rect.height * dpr;
    const ctx = canvas.getContext("2d");
    ctx.scale(dpr, dpr);
    return { ctx, w: rect.width, h: rect.height };
  }

  function _clear(ctx, w, h) {
    ctx.clearRect(0, 0, w, h);
  }

  // ── Horizontal Bar Chart ──────────────────────────────────────────────────
  function drawBarChart(canvas, items, opts = {}) {
    const { ctx, w, h } = _dpi(canvas);
    _clear(ctx, w, h);
    if (!items || !items.length) return;

    const {
      valueKey = "value",
      labelKey = "label",
      color: barColor = PALETTE[0],
      maxLabel = 20,
      unit = "",
    } = opts;

    const BAR_H   = 22;
    const PAD_LEFT = 130;
    const PAD_RIGHT = 60;
    const PAD_TOP   = 10;
    const gap = 8;

    const visibleItems = items.slice(0, maxLabel);
    const maxVal = Math.max(...visibleItems.map(d => d[valueKey] || 0)) || 1;

    ctx.font = "12px -apple-system, sans-serif";

    visibleItems.forEach((item, i) => {
      const y = PAD_TOP + i * (BAR_H + gap);
      const barW = Math.max(2, ((item[valueKey] || 0) / maxVal) * (w - PAD_LEFT - PAD_RIGHT));
      const label = item[labelKey] || "";
      const val = item[valueKey] || 0;

      // Background bar
      ctx.fillStyle = "rgba(42,52,71,0.5)";
      ctx.beginPath();
      ctx.roundRect(PAD_LEFT, y, w - PAD_LEFT - PAD_RIGHT, BAR_H, 3);
      ctx.fill();

      // Fill bar
      const grad = ctx.createLinearGradient(PAD_LEFT, 0, PAD_LEFT + barW, 0);
      grad.addColorStop(0, barColor);
      grad.addColorStop(1, barColor + "99");
      ctx.fillStyle = grad;
      ctx.beginPath();
      ctx.roundRect(PAD_LEFT, y, barW, BAR_H, 3);
      ctx.fill();

      // Label
      ctx.fillStyle = "#8b98ab";
      ctx.textAlign = "right";
      ctx.fillText(label.length > 16 ? label.slice(0, 16) + "…" : label, PAD_LEFT - 8, y + BAR_H / 2 + 4);

      // Value
      ctx.fillStyle = "#e2e8f0";
      ctx.textAlign = "left";
      ctx.font = "bold 12px -apple-system, sans-serif";
      ctx.fillText(`${fmtCr(val)}${unit}`, PAD_LEFT + barW + 6, y + BAR_H / 2 + 4);
      ctx.font = "12px -apple-system, sans-serif";
    });

    canvas.style.height = (PAD_TOP + visibleItems.length * (BAR_H + gap)) + "px";
  }

  // ── Donut Chart ───────────────────────────────────────────────────────────
  function drawDonut(canvas, segments, opts = {}) {
    const { ctx, w, h } = _dpi(canvas);
    _clear(ctx, w, h);
    if (!segments || !segments.length) return;

    const { labelKey = "label", valueKey = "value", centerLabel = "" } = opts;
    const cx = w / 2, cy = h / 2;
    const R = Math.min(cx, cy) - 15;
    const r = R * 0.58;
    const total = segments.reduce((s, d) => s + (d[valueKey] || 0), 0) || 1;

    let angle = -Math.PI / 2;
    segments.forEach((seg, i) => {
      const sweep = (seg[valueKey] / total) * 2 * Math.PI;
      ctx.beginPath();
      ctx.moveTo(cx, cy);
      ctx.arc(cx, cy, R, angle, angle + sweep);
      ctx.closePath();
      ctx.fillStyle = PALETTE[i % PALETTE.length];
      ctx.fill();
      angle += sweep;
    });

    // Hole
    ctx.beginPath();
    ctx.arc(cx, cy, r, 0, 2 * Math.PI);
    ctx.fillStyle = "#161b22";
    ctx.fill();

    // Center text
    if (centerLabel) {
      ctx.fillStyle = "#e2e8f0";
      ctx.font = "bold 14px -apple-system, sans-serif";
      ctx.textAlign = "center";
      ctx.fillText(centerLabel, cx, cy + 5);
    }
  }

  // ── Line / Forecast Chart ─────────────────────────────────────────────────
  function drawForecast(canvas, data, opts = {}) {
    const { ctx, w, h } = _dpi(canvas);
    _clear(ctx, w, h);
    if (!data || !data.length) return;

    const PAD = { top: 20, right: 20, bottom: 36, left: 60 };
    const cw = w - PAD.left - PAD.right;
    const ch = h - PAD.top  - PAD.bottom;

    const n    = data.length;
    const maxY = Math.max(...data.map(d => Math.max(d.allocated || 0, d.utilized || 0, d.forecast || 0))) * 1.1 || 1;

    const xOf = i => PAD.left + (i / (n - 1)) * cw;
    const yOf = v => PAD.top + ch - (v / maxY) * ch;

    // Grid lines
    ctx.strokeStyle = "#2a3447";
    ctx.lineWidth = 1;
    for (let g = 0; g <= 4; g++) {
      const y = PAD.top + (g / 4) * ch;
      ctx.beginPath();
      ctx.moveTo(PAD.left, y); ctx.lineTo(PAD.left + cw, y);
      ctx.stroke();
      ctx.fillStyle = "#566474";
      ctx.font = "11px sans-serif";
      ctx.textAlign = "right";
      ctx.fillText(fmtCr(maxY * (4 - g) / 4), PAD.left - 6, y + 4);
    }

    // Allocated area
    ctx.beginPath();
    data.forEach((d, i) => {
      const x = xOf(i), y = yOf(d.allocated || 0);
      i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
    });
    ctx.strokeStyle = "#2a3447";
    ctx.lineWidth = 1.5;
    ctx.setLineDash([5, 4]);
    ctx.stroke();
    ctx.setLineDash([]);

    // Utilized line
    ctx.beginPath();
    data.forEach((d, i) => {
      if (d.utilized === undefined) return;
      const x = xOf(i), y = yOf(d.utilized);
      i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
    });
    ctx.strokeStyle = "#3b82f6";
    ctx.lineWidth = 2.5;
    ctx.stroke();

    // Forecast (dashed extension)
    const fData = data.filter(d => d.forecast !== undefined);
    if (fData.length) {
      ctx.beginPath();
      fData.forEach((d, i) => {
        // Connect from last actual point
        const globalIdx = data.indexOf(d);
        const x = xOf(globalIdx), y = yOf(d.forecast);
        i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
      });
      ctx.strokeStyle = "#22c55e";
      ctx.lineWidth = 2;
      ctx.setLineDash([6, 4]);
      ctx.stroke();
      ctx.setLineDash([]);
    }

    // Dots
    data.forEach((d, i) => {
      if (d.utilized !== undefined) {
        ctx.beginPath();
        ctx.arc(xOf(i), yOf(d.utilized), 4, 0, 2 * Math.PI);
        ctx.fillStyle = "#3b82f6";
        ctx.fill();
      }
    });

    // X-axis labels
    data.forEach((d, i) => {
      ctx.fillStyle = "#566474";
      ctx.font = "11px sans-serif";
      ctx.textAlign = "center";
      ctx.fillText(d.label || `Q${i + 1}`, xOf(i), PAD.top + ch + 20);
    });

    // Legend
    const legend = [
      { color: "#2a3447", label: "Allocated", dash: true },
      { color: "#3b82f6", label: "Utilized" },
      { color: "#22c55e", label: "Forecast", dash: true },
    ];
    let lx = PAD.left;
    const ly = PAD.top + ch + 32;
    legend.forEach(l => {
      ctx.strokeStyle = l.color;
      ctx.lineWidth = 2;
      if (l.dash) ctx.setLineDash([4, 3]); else ctx.setLineDash([]);
      ctx.beginPath();
      ctx.moveTo(lx, ly); ctx.lineTo(lx + 20, ly);
      ctx.stroke();
      ctx.setLineDash([]);
      ctx.fillStyle = "#8b98ab";
      ctx.font = "11px sans-serif";
      ctx.textAlign = "left";
      ctx.fillText(l.label, lx + 24, ly + 4);
      lx += 80;
    });
  }

  // ── Sankey (Bézier) Diagram ───────────────────────────────────────────────
  function drawSankey(canvas, nodes, links, opts = {}) {
    const { ctx, w, h } = _dpi(canvas);
    _clear(ctx, w, h);
    if (!links || !links.length) return;

    const PAD = { top: 20, bottom: 20, left: 20, right: 20 };
    const ugh = h - PAD.top - PAD.bottom;
    const ugw = w - PAD.left - PAD.right;

    // Collect unique sources and targets
    const sources = [...new Set(links.map(l => l.source))];
    const targets = [...new Set(links.map(l => l.target))];

    const totalValue = links.reduce((s, l) => s + (l.value || 0), 0) || 1;

    const NODE_W = 18;
    const srcX  = PAD.left + NODE_W / 2;
    const tgtX  = PAD.left + ugw - NODE_W / 2;

    // Source node positions
    const srcNodes = {};
    sources.forEach((s, i) => {
      const linkTotal = links.filter(l => l.source === s).reduce((a, l) => a + l.value, 0);
      srcNodes[s] = {
        y: PAD.top + (i + 0.5) * (ugh / sources.length),
        total: linkTotal,
        offset: 0,
      };
    });

    // Target node positions
    const tgtNodes = {};
    targets.forEach((t, i) => {
      const linkTotal = links.filter(l => l.target === t).reduce((a, l) => a + l.value, 0);
      tgtNodes[t] = {
        y: PAD.top + (i + 0.5) * (ugh / targets.length),
        total: linkTotal,
        offset: 0,
      };
    });

    const maxNodeH = (ugh / Math.max(sources.length, targets.length)) - 8;

    // Draw flows
    links.forEach((link, i) => {
      const sn = srcNodes[link.source];
      const tn = tgtNodes[link.target];
      if (!sn || !tn) return;

      const flowH = Math.max(2, (link.value / totalValue) * ugh * 0.85);
      const sy = sn.y - sn.total / totalValue * maxNodeH / 2 + sn.offset;
      const ty = tn.y - tn.total / totalValue * maxNodeH / 2 + tn.offset;

      sn.offset += flowH + 2;
      tn.offset += flowH + 2;

      const color = PALETTE[i % PALETTE.length];
      const mx = (srcX + tgtX) / 2;

      ctx.beginPath();
      ctx.moveTo(srcX + NODE_W / 2, sy);
      ctx.bezierCurveTo(mx, sy, mx, ty, tgtX - NODE_W / 2, ty);
      ctx.bezierCurveTo(mx, ty + flowH, mx, sy + flowH, srcX + NODE_W / 2, sy + flowH);
      ctx.closePath();
      ctx.fillStyle = color + "44";
      ctx.fill();
      ctx.strokeStyle = color + "88";
      ctx.lineWidth = 1;
      ctx.stroke();
    });

    // Draw source nodes
    sources.forEach((s, i) => {
      const sn = srcNodes[s];
      const nh = Math.min(maxNodeH, (sn.total / totalValue) * ugh * 0.85 + 10);
      const ny = sn.y - nh / 2;
      ctx.fillStyle = PALETTE[i % PALETTE.length];
      ctx.beginPath();
      ctx.roundRect(srcX - NODE_W / 2, ny, NODE_W, nh, 3);
      ctx.fill();
      // Label
      ctx.fillStyle = "#e2e8f0";
      ctx.font = "11px sans-serif";
      ctx.textAlign = "right";
      ctx.fillText(s.length > 14 ? s.slice(0, 13) + "…" : s, srcX - NODE_W / 2 - 6, sn.y + 4);
    });

    // Draw target nodes
    targets.forEach((t, i) => {
      const tn = tgtNodes[t];
      const nh = Math.min(maxNodeH, (tn.total / totalValue) * ugh * 0.85 + 10);
      const ny = tn.y - nh / 2;
      ctx.fillStyle = PALETTE[(i + sources.length) % PALETTE.length];
      ctx.beginPath();
      ctx.roundRect(tgtX - NODE_W / 2, ny, NODE_W, nh, 3);
      ctx.fill();
      ctx.fillStyle = "#e2e8f0";
      ctx.font = "11px sans-serif";
      ctx.textAlign = "left";
      ctx.fillText(t.length > 18 ? t.slice(0, 17) + "…" : t, tgtX + NODE_W / 2 + 6, tn.y + 4);
    });
  }

  // ── Utilization Heatmap (quarters × categories) ───────────────────────────
  function drawHeatmap(canvas, rows, cols, values, opts = {}) {
    const { ctx, w, h } = _dpi(canvas);
    _clear(ctx, w, h);
    if (!rows.length) return;

    const PAD_LEFT   = 120;
    const PAD_BOTTOM = 32;
    const PAD_TOP    = 10;
    const PAD_RIGHT  = 10;

    const cw = (w - PAD_LEFT - PAD_RIGHT) / cols.length;
    const rh = (h - PAD_TOP - PAD_BOTTOM) / rows.length;

    ctx.font = "11px sans-serif";

    rows.forEach((row, ri) => {
      cols.forEach((col, ci) => {
        const val = values[ri]?.[ci] ?? 0;
        const pct = Math.min(1, val / 100);
        const r = Math.round(20 + pct * 219);
        const g = Math.round(31 + pct * 166);
        const b = Math.round(42  + pct * 50);
        const alpha = 0.3 + pct * 0.7;

        const x = PAD_LEFT + ci * cw;
        const y = PAD_TOP  + ri * rh;

        ctx.fillStyle = `rgba(${r},${g},${b},${alpha})`;
        ctx.beginPath();
        ctx.roundRect(x + 2, y + 2, cw - 4, rh - 4, 4);
        ctx.fill();

        ctx.fillStyle = val > 50 ? "#e2e8f0" : "#8b98ab";
        ctx.textAlign = "center";
        ctx.fillText(`${Math.round(val)}%`, x + cw / 2, y + rh / 2 + 4);
      });

      // Row label
      ctx.fillStyle = "#8b98ab";
      ctx.textAlign = "right";
      ctx.fillText(row.length > 14 ? row.slice(0, 13) + "…" : row,
                   PAD_LEFT - 6, PAD_TOP + ri * rh + rh / 2 + 4);
    });

    // Column labels
    cols.forEach((col, ci) => {
      ctx.fillStyle = "#566474";
      ctx.textAlign = "center";
      ctx.fillText(col, PAD_LEFT + ci * cw + cw / 2, PAD_TOP + rows.length * rh + 20);
    });
  }

  // ── Utility ──────────────────────────────────────────────────────────────
  function fmtCr(v) {
    if (v >= 1e5)   return (v / 1e5).toFixed(1)   + "L Cr";
    if (v >= 1e3)   return (v / 1e3).toFixed(1)   + "K Cr";
    return Math.round(v) + " Cr";
  }

  return { drawBarChart, drawDonut, drawForecast, drawSankey, drawHeatmap, PALETTE };
})();
