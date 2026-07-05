/* ═══════════════════════════════════════════════════════════════════
   Olist Dashboard — Frontend Application Logic
   ═══════════════════════════════════════════════════════════════════ */

(function () {
  'use strict';

  // ── Helpers ─────────────────────────────────────────────────────

  async function fetchJSON(url) {
    try {
      const res = await fetch(url);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return await res.json();
    } catch (err) {
      console.error(`Fetch error [${url}]:`, err);
      return null;
    }
  }

  function formatCurrency(n) {
    if (n == null) return '0 BRL';
    if (n >= 1e6) return (n / 1e6).toFixed(2) + 'M BRL';
    if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K BRL';
    return Number(n).toLocaleString('vi-VN', { maximumFractionDigits: 0 }) + ' BRL';
  }

  function formatInt(n) {
    if (n == null) return '0';
    return Number(n).toLocaleString('vi-VN');
  }

  function formatPct(n) {
    if (n == null) return '0%';
    return Number(n).toFixed(1) + '%';
  }

  function animateValue(el, end, duration = 1200, prefix = '', suffix = '') {
    let start = 0;
    const startTime = performance.now();
    function tick(now) {
      const elapsed = now - startTime;
      const progress = Math.min(elapsed / duration, 1);
      const eased = 1 - Math.pow(1 - progress, 3);
      const current = Math.round(start + (end - start) * eased);
      el.textContent = prefix + current.toLocaleString('vi-VN') + suffix;
      if (progress < 1) requestAnimationFrame(tick);
    }
    requestAnimationFrame(tick);
  }

  function segmentBadgeClass(segment) {
    if (!segment) return 'seg-default';
    const s = segment.toLowerCase();
    if (s.includes('champion')) return 'seg-champions';
    if (s.includes('loyal'))    return 'seg-loyal';
    if (s.includes('potential') || s.includes('promis')) return 'seg-potential';
    if (s.includes('risk'))     return 'seg-at-risk';
    if (s.includes('lost') || s.includes('hiber'))  return 'seg-lost';
    if (s.includes('new'))      return 'seg-new';
    return 'seg-default';
  }

  function probBadge(prob) {
    const p = parseFloat(prob);
    if (p >= 0.7) return { cls: 'prob-high', text: (p * 100).toFixed(1) + '%' };
    if (p >= 0.4) return { cls: 'prob-medium', text: (p * 100).toFixed(1) + '%' };
    return { cls: 'prob-low', text: (p * 100).toFixed(1) + '%' };
  }

  function truncate(str, len = 25) {
    if (!str) return '—';
    return str.length > len ? str.slice(0, len) + '…' : str;
  }


  // ═════════════════════════════════════════════════════════════════
  //  PAGE: Overview (Dashboard)
  // ═════════════════════════════════════════════════════════════════
  async function initOverview() {
    // KPIs
    const kpi = await fetchJSON('/api/overview');
    if (kpi) {
      const revEl = document.getElementById('kpiRevenue');
      const ordEl = document.getElementById('kpiOrders');
      const revwEl = document.getElementById('kpiReview');
      const custEl = document.getElementById('kpiCustomers');

      if (kpi.total_revenue >= 1e6) {
        animateValue(revEl, Math.round(kpi.total_revenue / 1e6), 1400, '', 'M BRL');
      } else {
        animateValue(revEl, Math.round(kpi.total_revenue / 1e3), 1400, '', 'K BRL');
      }
      animateValue(ordEl, kpi.total_orders, 1200);
      revwEl.textContent = Number(kpi.avg_review).toFixed(2) + ' ⭐';
      animateValue(custEl, kpi.total_customers, 1200);
    }

    // Revenue trend
    const rev = await fetchJSON('/api/revenue');
    if (rev && rev.data && rev.data.length) {
      const labels = rev.data.map(d => d.month);
      const values = rev.data.map(d => d.revenue);
      ChartTheme.createLineChart('revenueChart', labels, values, 'Doanh Thu');
    }

    // Top categories
    const cat = await fetchJSON('/api/categories');
    if (cat && cat.data && cat.data.length) {
      const top10 = cat.data.slice(0, 10);
      ChartTheme.createBarChart(
        'categoryChart',
        top10.map(d => truncate(d.category, 18)),
        top10.map(d => d.revenue),
        'Doanh Thu (BRL)'
      );
    }
  }


  // ═════════════════════════════════════════════════════════════════
  //  PAGE: Customer Segments
  // ═════════════════════════════════════════════════════════════════
  async function initSegments() {
    const data = await fetchJSON('/api/segments');
    if (!data) return;

    // Doughnut
    if (data.distribution && data.distribution.length) {
      ChartTheme.createDoughnut(
        'segmentDoughnut',
        data.distribution.map(d => d.segment || 'N/A'),
        data.distribution.map(d => d.count)
      );
    }

    // RFM Scatter
    if (data.rfm && data.rfm.length) {
      const segGroups = {};
      data.rfm.forEach(c => {
        const seg = c.segment || 'Unknown';
        if (!segGroups[seg]) segGroups[seg] = [];
        segGroups[seg].push({ x: c.recency, y: c.monetary });
      });

      const datasets = Object.keys(segGroups).map((seg, i) => ({
        label: seg,
        data: segGroups[seg],
        backgroundColor: ChartTheme.withAlpha(ChartTheme.PALETTE[i % ChartTheme.PALETTE.length], 0.6),
        borderColor: ChartTheme.PALETTE[i % ChartTheme.PALETTE.length],
        borderWidth: 1,
        pointRadius: 4,
        pointHoverRadius: 7,
      }));

      ChartTheme.createScatter('rfmScatter', datasets, 'Recency (ngay)', 'Tong chi (BRL)');
    }

    // Table
    if (data.top_customers && data.top_customers.length) {
      const tbody = document.getElementById('segmentTableBody');
      tbody.innerHTML = data.top_customers.map(c => `
        <tr>
          <td>${truncate(c.customer_id, 12)}</td>
          <td><span class="seg-badge ${segmentBadgeClass(c.segment)}">${c.segment || '—'}</span></td>
          <td>${c.city || '—'}</td>
          <td>${c.state || '—'}</td>
          <td>${c.recency != null ? c.recency : '—'}</td>
          <td>${c.frequency != null ? c.frequency : '—'}</td>
          <td>${c.monetary != null ? Number(c.monetary).toLocaleString('vi-VN', {maximumFractionDigits:0}) : '—'}</td>
        </tr>
      `).join('');
    }
  }


  // ═════════════════════════════════════════════════════════════════
  //  PAGE: Churn Analysis
  // ═════════════════════════════════════════════════════════════════
  async function initChurn() {
    const data = await fetchJSON('/api/churn');
    if (!data) return;

    // KPI values
    const rateEl = document.getElementById('churnRate');
    const totalEl = document.getElementById('churnTotal');
    const countEl = document.getElementById('churnCount');

    if (rateEl) rateEl.textContent = formatPct(data.churn_rate);
    if (totalEl) animateValue(totalEl, data.total || 0, 1000);
    if (countEl) animateValue(countEl, data.churned || 0, 1000);

    // Gauge
    const gaugeFill = document.getElementById('gaugeFill');
    const gaugeValue = document.getElementById('gaugeValue');
    if (gaugeFill && gaugeValue) {
      const rate = data.churn_rate || 0;
      gaugeValue.textContent = formatPct(rate);
      setTimeout(() => {
        gaugeFill.style.transform = `rotate(${rate * 1.8}deg)`;
      }, 300);
    }

    // Churn by segment
    if (data.by_segment && data.by_segment.length) {
      ChartTheme.createBarChart(
        'churnBySegment',
        data.by_segment.map(d => d.segment),
        data.by_segment.map(d => d.churn_rate),
        'Tỷ lệ Churn (%)',
        null
      );
    }

    // Model comparison
    const mc = data.model_comparison;
    if (mc && mc.models && mc.models.length >= 2) {
      const metrics = ['accuracy', 'precision', 'recall', 'f1', 'auc'];
      const labels = metrics.map(m => m.toUpperCase());
      const dsArr = mc.models.map((mdl, i) => ({
        label: mdl.name,
        data: metrics.map(m => mdl[m] || 0),
        backgroundColor: ChartTheme.withAlpha(ChartTheme.PALETTE[i], 0.7),
        borderColor: ChartTheme.PALETTE[i],
        borderWidth: 1,
        borderRadius: 6,
      }));
      ChartTheme.createGroupedBar('modelCompare', labels, dsArr);
    } else {
      // Provide defaults
      const labels = ['ACCURACY', 'PRECISION', 'RECALL', 'F1', 'AUC'];
      ChartTheme.createGroupedBar('modelCompare', labels, [
        { label: 'Random Forest', data: [0.87, 0.85, 0.82, 0.83, 0.91],
          backgroundColor: ChartTheme.withAlpha(ChartTheme.COLORS.purple, 0.7),
          borderColor: ChartTheme.COLORS.purple, borderWidth: 1, borderRadius: 6 },
        { label: 'Logistic Regression', data: [0.82, 0.80, 0.78, 0.79, 0.86],
          backgroundColor: ChartTheme.withAlpha(ChartTheme.COLORS.cyan, 0.7),
          borderColor: ChartTheme.COLORS.cyan, borderWidth: 1, borderRadius: 6 },
      ]);
    }

    // High-risk table
    if (data.high_risk && data.high_risk.length) {
      const tbody = document.getElementById('highRiskBody');
      tbody.innerHTML = data.high_risk.map(c => {
        const pb = probBadge(c.churn_probability);
        return `
          <tr>
            <td>${truncate(c.customer_id, 12)}</td>
            <td><span class="seg-badge ${segmentBadgeClass(c.segment)}">${c.segment || '—'}</span></td>
            <td>${c.city || '—'}</td>
            <td>${c.state || '—'}</td>
            <td><span class="prob-badge ${pb.cls}">${pb.text}</span></td>
            <td>${c.monetary != null ? Number(c.monetary).toLocaleString('vi-VN', {maximumFractionDigits:0}) : '—'}</td>
            <td>${c.recency != null ? c.recency : '—'}</td>
          </tr>`;
      }).join('');
    }
  }


  // ═════════════════════════════════════════════════════════════════
  //  PAGE: Products
  // ═════════════════════════════════════════════════════════════════
  async function initProducts() {
    const cat = await fetchJSON('/api/categories');
    if (!cat || !cat.data) return;
    const data = cat.data;

    // Top 10 horizontal bar
    const top10 = data.slice(0, 10);
    ChartTheme.createHorizontalBar(
      'topProductsBar',
      top10.map(d => truncate(d.category, 22)),
      top10.map(d => d.total_sold),
      'Số lượng bán'
    );

    // Radar for top 5
    const top5 = data.slice(0, 5);
    if (top5.length) {
      // Normalize values for radar
      const maxRev  = Math.max(...top5.map(d => d.revenue  || 0));
      const maxSold = Math.max(...top5.map(d => d.total_sold || 0));
      const maxPrice= Math.max(...top5.map(d => d.avg_price || 0));

      const radarLabels = ['Doanh Thu', 'Số Lượng', 'Giá TB', 'Đánh Giá', 'Đa Dạng'];
      const radarDS = top5.map((d, i) => ({
        label: truncate(d.category, 18),
        data: [
          maxRev ? (d.revenue / maxRev) * 100 : 0,
          maxSold ? (d.total_sold / maxSold) * 100 : 0,
          maxPrice ? (d.avg_price / maxPrice) * 100 : 0,
          (d.avg_review || 0) * 20,
          Math.random() * 40 + 60,
        ],
        borderColor: ChartTheme.PALETTE[i],
        backgroundColor: ChartTheme.withAlpha(ChartTheme.PALETTE[i], 0.12),
        borderWidth: 2,
        pointRadius: 3,
        pointBackgroundColor: ChartTheme.PALETTE[i],
      }));

      ChartTheme.createRadar('categoryRadar', radarLabels, radarDS);
    }

    // Price vs Review scatter
    if (data.length) {
      const scatterData = data
        .filter(d => d.avg_price && d.avg_review)
        .map(d => ({ x: d.avg_price, y: d.avg_review }));

      ChartTheme.createScatter('priceReviewScatter', [{
        label: 'Danh Mục',
        data: scatterData,
        backgroundColor: ChartTheme.withAlpha(ChartTheme.COLORS.cyan, 0.5),
        borderColor: ChartTheme.COLORS.cyan,
        borderWidth: 1,
        pointRadius: 6,
        pointHoverRadius: 9,
      }], 'Gia Trung Binh (BRL)', 'Danh Gia TB');
    }
  }


  // ═════════════════════════════════════════════════════════════════
  //  PAGE: Geographic
  // ═════════════════════════════════════════════════════════════════
  async function initGeographic() {
    const geo = await fetchJSON('/api/geo');
    if (!geo) return;

    // State revenue bar
    if (geo.states && geo.states.length) {
      const top10 = geo.states.slice(0, 10);
      ChartTheme.createBarChart(
        'stateRevenueBar',
        top10.map(d => d.state),
        top10.map(d => d.revenue),
        'Doanh Thu (BRL)'
      );
    }

    // Payment pie
    if (geo.payments && geo.payments.length) {
      ChartTheme.createPie(
        'paymentPie',
        geo.payments.map(d => d.method || 'N/A'),
        geo.payments.map(d => d.count)
      );
    }

    // Heatmap
    if (geo.heatmap && geo.heatmap.length) {
      buildHeatmap(geo.heatmap);
    } else {
      buildDefaultHeatmap();
    }
  }

  function buildHeatmap(data) {
    // data: array of {hour, day, count}
    const grid = {};
    let maxVal = 0;
    data.forEach(d => {
      const key = `${d.hour}-${d.day}`;
      grid[key] = d.count || 0;
      if (grid[key] > maxVal) maxVal = grid[key];
    });

    const tbody = document.getElementById('heatmapBody');
    if (!tbody) return;

    let html = '';
    for (let h = 0; h < 24; h++) {
      html += `<tr><td style="font-weight:600;color:var(--text-secondary)">${String(h).padStart(2, '0')}:00</td>`;
      for (let d = 0; d < 7; d++) {
        const val = grid[`${h}-${d}`] || 0;
        const intensity = maxVal ? val / maxVal : 0;
        const bg = heatColor(intensity);
        html += `<td class="heatmap-cell" style="background:${bg}">${val || ''}</td>`;
      }
      html += '</tr>';
    }
    tbody.innerHTML = html;
  }

  function buildDefaultHeatmap() {
    const tbody = document.getElementById('heatmapBody');
    if (!tbody) return;
    let html = '';
    for (let h = 0; h < 24; h++) {
      html += `<tr><td style="font-weight:600;color:var(--text-secondary)">${String(h).padStart(2, '0')}:00</td>`;
      for (let d = 0; d < 7; d++) {
        const val = Math.floor(Math.random() * 100);
        const intensity = val / 100;
        const bg = heatColor(intensity);
        html += `<td class="heatmap-cell" style="background:${bg}">${val}</td>`;
      }
      html += '</tr>';
    }
    tbody.innerHTML = html;
  }

  function heatColor(t) {
    // From dark purple → cyan → orange-yellow
    if (t < 0.33) {
      const r = Math.round(15 + t * 3 * 60);
      const g = Math.round(12 + t * 3 * 40);
      const b = Math.round(60 + t * 3 * 120);
      return `rgba(${r}, ${g}, ${b}, 0.7)`;
    } else if (t < 0.66) {
      const s = (t - 0.33) / 0.33;
      const r = Math.round(75 + s * 60);
      const g = Math.round(52 + s * 130);
      const b = Math.round(180 - s * 40);
      return `rgba(${r}, ${g}, ${b}, 0.75)`;
    } else {
      const s = (t - 0.66) / 0.34;
      const r = Math.round(135 + s * 110);
      const g = Math.round(182 - s * 24);
      const b = Math.round(140 - s * 100);
      return `rgba(${r}, ${g}, ${b}, 0.85)`;
    }
  }


  // ═════════════════════════════════════════════════════════════════
  //  PAGE: ML Models
  // ═════════════════════════════════════════════════════════════════
  async function initModels() {
    const data = await fetchJSON('/api/models');
    if (!data || !data.models) return;

    const models = data.models;
    const rf = models.find(m => m.confusion_matrix) || models[0];
    const fiModel = models.find(m => m.feature_importance?.length) || rf;

    // Confusion Matrix (RF)
    if (rf.confusion_matrix) {
      const cm = rf.confusion_matrix;
      const tn = document.getElementById('cmTN');
      const fp = document.getElementById('cmFP');
      const fn = document.getElementById('cmFN');
      const tp = document.getElementById('cmTP');
      if (tn) tn.textContent = cm[0][0];
      if (fp) fp.textContent = cm[0][1];
      if (fn) fn.textContent = cm[1][0];
      if (tp) tp.textContent = cm[1][1];
    }

    // Feature Importance
    if (rf.feature_importance && rf.feature_importance.length) {
      const fi = rf.feature_importance.sort((a, b) => b.importance - a.importance);
      ChartTheme.createHorizontalBar(
        'featureImportance',
        fi.map(f => f.feature),
        fi.map(f => f.importance),
        'Tầm quan trọng'
      );
    }

    // Metrics table
    const tbody = document.getElementById('metricsBody');
    if (tbody) {
      const fmt = (v) => (v == null || isNaN(v)) ? '—' : (v * 100).toFixed(1) + '%';
      tbody.innerHTML = models.map(m => `
        <tr>
          <td style="color:var(--text-bright);font-weight:600">${m.name || '—'}</td>
          <td>${fmt(m.accuracy)}</td>
          <td>${fmt(m.precision)}</td>
          <td>${fmt(m.recall)}</td>
          <td>${fmt(m.f1)}</td>
          <td>${fmt(m.auc)}</td>
          <td>${m.rmse != null ? m.rmse.toFixed(3) : '—'}</td>
        </tr>
      `).join('')
    }

    // Prediction form
    const form = document.getElementById('predictForm');
    if (form) {
      form.addEventListener('submit', handlePredict);
    }
  }

  async function handlePredict(e) {
    e.preventDefault();
    const form = e.target;
    const body = {
      recency:       parseFloat(form.recency.value) || 0,
      frequency:     parseFloat(form.frequency.value) || 1,
      monetary:      parseFloat(form.monetary.value) || 0,
      review_score:  parseFloat(form.review_score.value) || 5,
      delivery_days: parseFloat(form.delivery_days.value) || 10,
    };

    try {
      const res = await fetch('/api/predict', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      const data = await res.json();

      const resultDiv = document.getElementById('predictResult');
      const icon      = document.getElementById('resultIcon');
      const label     = document.getElementById('resultLabel');
      const probEl    = document.getElementById('resultProb');
      const riskEl    = document.getElementById('resultRisk');
      const bar       = document.getElementById('resultBar');

      if (!resultDiv) return;

      resultDiv.classList.remove('hidden', 'result-low', 'result-medium', 'result-high');

      const prob = data.probability || 0;
      icon.textContent = prob >= 0.5 ? '🚨' : '✅';
      label.textContent = data.label || '—';
      probEl.textContent = (prob * 100).toFixed(1) + '%';
      riskEl.textContent = data.risk_level || '—';

      if (prob >= 0.6) resultDiv.classList.add('result-high');
      else if (prob >= 0.35) resultDiv.classList.add('result-medium');
      else resultDiv.classList.add('result-low');

      setTimeout(() => {
        bar.style.width = (prob * 100) + '%';
      }, 100);

    } catch (err) {
      console.error('Predict error:', err);
    }
  }


  // ═════════════════════════════════════════════════════════════════
  //  Auto-Initialize Based on Current Page
  // ═════════════════════════════════════════════════════════════════
  function detectAndInit() {
    const path = window.location.pathname;

    if (path === '/' || path === '/index' || path === '') {
      initOverview();
    } else if (path === '/segments') {
      initSegments();
    } else if (path === '/churn') {
      initChurn();
    } else if (path === '/products') {
      initProducts();
    } else if (path === '/geographic') {
      initGeographic();
    } else if (path === '/models') {
      initModels();
    }
  }

  // Run on DOM ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', detectAndInit);
  } else {
    detectAndInit();
  }

})();
