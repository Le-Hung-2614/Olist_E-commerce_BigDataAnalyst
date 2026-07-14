/* ═══════════════════════════════════════════════════════════════════
   Olist Dashboard — Frontend Application Logic
   ═══════════════════════════════════════════════════════════════════ */

(function () {
  'use strict';

  // ── Helpers ─────────────────────────────────────────────────────

  async function fetchJSON(url) {
    try {
      const sep = url.includes('?') ? '&' : '?';
      const fetchUrl = `${url}${sep}lang=${window.LANG_MODE}`;
      const res = await fetch(fetchUrl);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return await res.json();
    } catch (err) {
      console.error(`Fetch error [${url}]:`, err);
      return null;
    }
  }

  window.CURRENCY_MODE = localStorage.getItem('currency_mode') || 'BRL';
  const VND_RATE = 5140; // 1 BRL ~ 5140 VND

  function convertCur(n) { return window.CURRENCY_MODE === 'VND' ? n * VND_RATE : n; }

  function formatCurrency(n) {
    if (n == null) return window.CURRENCY_MODE === 'VND' ? '0 VNĐ' : '0 BRL';
    let val = convertCur(n);
    let sfx = window.CURRENCY_MODE === 'VND' ? ' VNĐ' : ' BRL';
    
    if (window.CURRENCY_MODE === 'VND') {
      if (val >= 1e9) return (val / 1e9).toFixed(2) + ' Tỷ' + sfx;
      if (val >= 1e6) return (val / 1e6).toFixed(2) + ' Tr' + sfx;
    } else {
      if (val >= 1e6) return (val / 1e6).toFixed(2) + 'M' + sfx;
      if (val >= 1e3) return (val / 1e3).toFixed(1) + 'K' + sfx;
    }
    return Number(val).toLocaleString('vi-VN', { maximumFractionDigits: 0 }) + sfx;
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
    if (s.includes('vip') || s.includes('champion')) return 'seg-champions';
    if (s.includes('trung thành') || s.includes('loyal')) return 'seg-loyal';
    if (s.includes('tiềm năng') || s.includes('hứa hẹn') || s.includes('potential') || s.includes('promis')) return 'seg-potential';
    if (s.includes('nguy cơ') || s.includes('risk')) return 'seg-at-risk';
    if (s.includes('mất') || s.includes('đông') || s.includes('lost') || s.includes('hiber')) return 'seg-lost';
    if (s.includes('mới') || s.includes('new')) return 'seg-new';
    return 'seg-default';
  }

  const SEGMENT_VI = {
    'Champions': 'Khách Hàng VIP',
    'Loyal': 'Trung Thành',
    'Potential Loyalist': 'Tiềm Năng',
    'Promising': 'Đầy Hứa Hẹn',
    'New Customers': 'Khách Mới',
    'At Risk': 'Nguy Cơ Rời Bỏ',
    'Lost': 'Đã Ngừng Mua',
    'Hibernating': 'Đang Ngủ Đông',
    'About To Sleep': 'Sắp Ngủ Đông',
    "Can't Lose Them": 'Không Thể Mất'
  };

  window.LANG_MODE = localStorage.getItem('lang_mode') || 'VI';

  function translateSegment(seg) {
    if (!seg) return 'N/A';
    if (window.LANG_MODE === 'EN') return seg;
    if (SEGMENT_VI[seg]) return SEGMENT_VI[seg];
    for (const [en, vi] of Object.entries(SEGMENT_VI)) {
      if (seg.toLowerCase().includes(en.toLowerCase())) return vi;
    }
    return seg;
  }

  const BRAZIL_STATES = {
    'AC': 'Acre', 'AL': 'Alagoas', 'AP': 'Amapá', 'AM': 'Amazonas', 'BA': 'Bahia',
    'CE': 'Ceará', 'DF': 'Distrito Federal', 'ES': 'Espírito Santo', 'GO': 'Goiás',
    'MA': 'Maranhão', 'MT': 'Mato Grosso', 'MS': 'Mato Grosso do Sul', 'MG': 'Minas Gerais',
    'PA': 'Pará', 'PB': 'Paraíba', 'PR': 'Paraná', 'PE': 'Pernambuco', 'PI': 'Piauí',
    'RJ': 'Rio de Janeiro', 'RN': 'Rio Grande do Norte', 'RS': 'Rio Grande do Sul',
    'RO': 'Rondônia', 'RR': 'Roraima', 'SC': 'Santa Catarina', 'SP': 'São Paulo',
    'SE': 'Sergipe', 'TO': 'Tocantins'
  };

  const PAYMENT_VI = {
    'credit_card': 'Thẻ Tín Dụng',
    'boleto': 'Thanh Toán Boleto',
    'voucher': 'Mã Giảm Giá',
    'debit_card': 'Thẻ Ghi Nợ',
    'unknown': 'Không Xác Định'
  };

  function translatePayment(method) {
    if (!method) return 'N/A';
    if (window.LANG_MODE === 'EN') return method;
    return PAYMENT_VI[method.toLowerCase()] || method;
  }

  function getFullStateName(stateCode) {
    if (!stateCode) return 'Unknown';
    if (window.LANG_MODE === 'EN') return stateCode;
    return BRAZIL_STATES[stateCode.toUpperCase()] || stateCode;
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

      const cardRev = revEl.closest('.kpi-card');
      if (cardRev) {
        cardRev.style.cursor = 'pointer';
        cardRev.title = 'Click để đổi đơn vị tiền (BRL ↔ VNĐ)';
        cardRev.onclick = () => {
          window.CURRENCY_MODE = window.CURRENCY_MODE === 'BRL' ? 'VND' : 'BRL';
          localStorage.setItem('currency_mode', window.CURRENCY_MODE);
          location.reload();
        };
      }

      const val = convertCur(kpi.total_revenue);
      const sfx = window.CURRENCY_MODE === 'VND' ? ' VNĐ' : ' BRL';
      
      if (window.CURRENCY_MODE === 'VND') {
        if (val >= 1e9) animateValue(revEl, Math.round(val / 1e9), 1400, '', ' Tỷ' + sfx);
        else animateValue(revEl, Math.round(val / 1e6), 1400, '', ' Tr' + sfx);
      } else {
        if (val >= 1e6) animateValue(revEl, Math.round(val / 1e6), 1400, '', 'M' + sfx);
        else animateValue(revEl, Math.round(val / 1e3), 1400, '', 'K' + sfx);
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
        window.CURRENCY_MODE === 'VND' ? 'Doanh Thu (VNĐ)' : 'Doanh Thu (BRL)'
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
      const order = ['Champions', 'Loyal', 'At Risk', 'Lost'];
      data.distribution.sort((a, b) => {
        let idxA = order.indexOf(a.segment);
        let idxB = order.indexOf(b.segment);
        if (idxA === -1) idxA = 99;
        if (idxB === -1) idxB = 99;
        return idxA - idxB;
      });

      ChartTheme.createDoughnut(
        'segmentDoughnut',
        data.distribution.map(d => translateSegment(d.segment || 'N/A')),
        data.distribution.map(d => d.count)
      );
    }

    // RFM Scatter
    if (data.rfm && data.rfm.length) {
      const segGroups = {};
      const order = ['Champions', 'Loyal', 'At Risk', 'Lost'];
      order.forEach(seg => {
        segGroups[translateSegment(seg)] = [];
      });
      
      data.rfm.forEach(c => {
        const seg = translateSegment(c.segment || 'Unknown');
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

      ChartTheme.createScatter('rfmScatter', datasets, 'Recency (ngày)', window.CURRENCY_MODE === 'VND' ? 'Tổng chi (VNĐ)' : 'Tổng chi (BRL)');
    }

    // Revenue Doughnut
    if (data.revenue_data && data.revenue_data.length) {
      const order = ['Champions', 'Loyal', 'At Risk', 'Lost'];
      data.revenue_data.sort((a, b) => {
        let idxA = order.indexOf(a.segment);
        let idxB = order.indexOf(b.segment);
        if (idxA === -1) idxA = 99;
        if (idxB === -1) idxB = 99;
        return idxA - idxB;
      });

      ChartTheme.createDoughnut(
        'revenueDoughnut',
        data.revenue_data.map(d => translateSegment(d.segment || 'N/A')),
        data.revenue_data.map(d => convertCur(d.revenue))
      );
    }

    // RFM Radar Chart
    if (data.radar_data && data.radar_data.length) {
      const order = ['Champions', 'Loyal', 'At Risk', 'Lost'];
      data.radar_data.sort((a, b) => {
        let idxA = order.indexOf(a._id);
        let idxB = order.indexOf(b._id);
        if (idxA === -1) idxA = 99;
        if (idxB === -1) idxB = 99;
        return idxA - idxB;
      });

      // Normalize data for radar chart so they are on the same scale (0 to 1)
      const maxR = Math.max(...data.radar_data.map(d => d.avg_recency));
      const maxF = Math.max(...data.radar_data.map(d => d.avg_frequency));
      const maxM = Math.max(...data.radar_data.map(d => d.avg_monetary));

      const datasets = data.radar_data.map((d, i) => {
        const segName = translateSegment(d._id || 'Unknown');
        const color = ChartTheme.PALETTE[i % ChartTheme.PALETTE.length];
        
        // Invert Recency because lower recency is better (ensure at least 5% height)
        const normR = Math.max(0.05, 1 - (d.avg_recency / maxR));
        const normF = Math.max(0.05, d.avg_frequency / maxF);
        const normM = Math.max(0.05, d.avg_monetary / maxM);
        
        return {
          label: segName,
          data: [normR, normF, normM],
          backgroundColor: ChartTheme.withAlpha(color, 0.2),
          borderColor: color,
          borderWidth: 2,
        };
      });

      ChartTheme.createGroupedBar('rfmRadarChart', ['Điểm Recency', 'Điểm Frequency', 'Điểm Monetary'], datasets);
    }

    // Trend by Segment
    if (data.trend_data && data.trend_data.length) {
      // Group by segment
      const segData = {};
      const order = ['Champions', 'Loyal', 'At Risk', 'Lost'];
      order.forEach(seg => {
        segData[translateSegment(seg)] = {};
      });

      // Get unique months sorted
      const months = [...new Set(data.trend_data.map(d => d.month))].sort();

      data.trend_data.forEach(d => {
        const seg = translateSegment(d.segment || 'Unknown');
        if (!segData[seg]) segData[seg] = {};
        segData[seg][d.month] = d.count;
      });

      const datasets = Object.keys(segData).map((seg, i) => {
        const lineData = months.map(m => segData[seg][m] || 0);
        return {
          label: seg,
          data: lineData,
          borderColor: ChartTheme.PALETTE[i % ChartTheme.PALETTE.length],
          backgroundColor: ChartTheme.withAlpha(ChartTheme.PALETTE[i % ChartTheme.PALETTE.length], 0.1),
          fill: false,
          tension: 0.4
        };
      });

      ChartTheme.createMultiLineChart('segmentTrendChart', months, datasets);
    }

    // Load table data separately
    loadSegmentTable(1, '');
    
    // Add event listener for search input
    const searchInput = document.getElementById('segmentSearchInput');
    if (searchInput) {
      let timeout = null;
      searchInput.addEventListener('input', (e) => {
        clearTimeout(timeout);
        timeout = setTimeout(() => {
          loadSegmentTable(1, e.target.value);
        }, 500); // Debounce 500ms
      });
    }
  }

  window.loadSegmentTable = async function(page, search) {
    const tbody = document.getElementById('segmentTableBody');
    if (tbody) tbody.innerHTML = '<tr><td colspan="7" class="loading-cell">Đang tải dữ liệu...</td></tr>';
    
    const data = await fetchJSON(`/api/customers/segment?page=${page}&limit=10&search=${encodeURIComponent(search)}`);
    if (!data) return;

    if (data.data && data.data.length && tbody) {
      tbody.innerHTML = data.data.map(c => `
        <tr>
          <td title="${c.customer_id}" style="cursor: pointer; color: var(--primary);" onclick="navigator.clipboard.writeText('${c.customer_id}').then(()=>alert('Đã copy mã khách hàng!'))" class="hover-copy">${truncate(c.customer_id, 12)}</td>
          <td><span class="seg-badge ${segmentBadgeClass(translateSegment(c.segment))}">${translateSegment(c.segment) || '—'}</span></td>
          <td>${c.city || '—'}</td>
          <td title="${getFullStateName(c.state)}">${c.state || '—'}</td>
          <td>${c.recency != null ? c.recency : '—'}</td>
          <td>${c.frequency != null ? c.frequency : '—'}</td>
          <td>${c.monetary != null ? formatCurrency(c.monetary) : '—'}</td>
        </tr>
      `).join('');
    } else if (tbody) {
      tbody.innerHTML = '<tr><td colspan="7" style="text-align:center; padding: 20px;">Không tìm thấy kết quả</td></tr>';
    }

    // Render pagination
    renderPagination('segmentPagination', data.page, data.total_pages, (newPage) => {
      loadSegmentTable(newPage, search);
    });
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
      const order = ['Champions', 'Loyal', 'At Risk', 'Lost'];
      data.by_segment.sort((a, b) => {
        let idxA = order.indexOf(a.segment);
        let idxB = order.indexOf(b.segment);
        if (idxA === -1) idxA = 99;
        if (idxB === -1) idxB = 99;
        return idxA - idxB;
      });

      ChartTheme.createBarChart(
        'churnBySegment',
        data.by_segment.map(d => translateSegment(d.segment)),
        data.by_segment.map(d => d.churn_rate),
        'Tỷ lệ Churn (%)',
        null
      );
    }

    // Top 5 States by Churn
    if (data.by_state && data.by_state.length) {
      ChartTheme.createBarChart(
        'churnByState',
        data.by_state.map(d => getFullStateName(d.state)),
        data.by_state.map(d => d.churned),
        'Số khách rời bỏ',
        null
      );
    }

    // Load table data separately
    loadChurnTable(1, '');
    
    // Add event listener for search input
    const searchInput = document.getElementById('churnSearchInput');
    if (searchInput) {
      let timeout = null;
      searchInput.addEventListener('input', (e) => {
        clearTimeout(timeout);
        timeout = setTimeout(() => {
          loadChurnTable(1, e.target.value);
        }, 500); // Debounce 500ms
      });
    }
  }

  window.loadChurnTable = async function(page, search) {
    const tbody = document.getElementById('highRiskBody');
    if (tbody) tbody.innerHTML = '<tr><td colspan="7" class="loading-cell">Đang tải dữ liệu...</td></tr>';
    
    const data = await fetchJSON(`/api/customers/churn?page=${page}&limit=10&search=${encodeURIComponent(search)}`);
    if (!data) return;

    if (data.data && data.data.length && tbody) {
      tbody.innerHTML = data.data.map(c => {
        const pb = probBadge(c.churn_probability);
        return `
          <tr>
            <td title="${c.customer_id}" style="cursor: pointer; color: var(--primary);" onclick="navigator.clipboard.writeText('${c.customer_id}').then(()=>alert('Đã copy mã khách hàng!'))" class="hover-copy">${truncate(c.customer_id, 12)}</td>
            <td><span class="seg-badge ${segmentBadgeClass(translateSegment(c.segment))}">${translateSegment(c.segment) || '—'}</span></td>
            <td>${c.city || '—'}</td>
            <td title="${getFullStateName(c.state)}">${c.state || '—'}</td>
            <td><span class="prob-badge ${pb.cls}">${pb.text}</span></td>
            <td>${c.monetary != null ? formatCurrency(c.monetary) : '—'}</td>
            <td>${c.recency != null ? c.recency : '—'}</td>
          </tr>`;
      }).join('');
    } else if (tbody) {
      tbody.innerHTML = '<tr><td colspan="7" style="text-align:center; padding: 20px;">Không tìm thấy kết quả</td></tr>';
    }

    // Render pagination
    renderPagination('churnPagination', data.page, data.total_pages, (newPage) => {
      loadChurnTable(newPage, search);
    });
  }

  function renderPagination(containerId, currentPage, totalPages, onPageClick) {
    const container = document.getElementById(containerId);
    if (!container) return;
    
    if (totalPages <= 1) {
      container.innerHTML = '';
      return;
    }

    let html = '';
    
    // Prev button
    if (currentPage > 1) {
      html += `<button class="page-btn" data-page="${currentPage - 1}">&laquo;</button>`;
    } else {
      html += `<button class="page-btn disabled" disabled>&laquo;</button>`;
    }

    // Page numbers
    const startPage = Math.max(1, currentPage - 2);
    const endPage = Math.min(totalPages, currentPage + 2);
    
    if (startPage > 1) {
      html += `<button class="page-btn" data-page="1">1</button>`;
      if (startPage > 2) html += `<span class="page-dots">...</span>`;
    }

    for (let i = startPage; i <= endPage; i++) {
      html += `<button class="page-btn ${i === currentPage ? 'active' : ''}" data-page="${i}">${i}</button>`;
    }

    if (endPage < totalPages) {
      if (endPage < totalPages - 1) html += `<span class="page-dots">...</span>`;
      html += `<button class="page-btn" data-page="${totalPages}">${totalPages}</button>`;
    }

    // Next button
    if (currentPage < totalPages) {
      html += `<button class="page-btn" data-page="${currentPage + 1}">&raquo;</button>`;
    } else {
      html += `<button class="page-btn disabled" disabled>&raquo;</button>`;
    }

    container.innerHTML = html;
    
    // Add click events
    container.querySelectorAll('button[data-page]').forEach(btn => {
      btn.addEventListener('click', () => {
        if (!btn.classList.contains('disabled')) {
          onPageClick(parseInt(btn.getAttribute('data-page')));
        }
      });
    });

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
        .map(d => ({ 
          x: window.CURRENCY_MODE === 'VND' ? d.avg_price * 7500 : d.avg_price, 
          y: d.avg_review, 
          name: d.category || 'Unknown'
        }));

      ChartTheme.createScatter('priceReviewScatter', [{
        label: 'Danh Mục',
        data: scatterData,
        backgroundColor: ChartTheme.withAlpha(ChartTheme.COLORS.cyan, 0.5),
        borderColor: ChartTheme.COLORS.cyan,
        borderWidth: 1,
        pointRadius: 6,
        pointHoverRadius: 9,
      }], window.CURRENCY_MODE === 'VND' ? 'Giá Trung Bình (VNĐ)' : 'Giá Trung Bình (BRL)', 'Đánh Giá TB', 
      (ctx) => {
        const pt = ctx.raw || scatterData[ctx.dataIndex] || {};
        const catName = pt.name || scatterData[ctx.dataIndex]?.name || 'Danh mục';
        const formattedX = window.CURRENCY_MODE === 'VND' 
            ? (pt.x || 0).toLocaleString('vi-VN') + ' ₫'
            : 'R$ ' + (pt.x || 0).toLocaleString('pt-BR');
        return `${catName}: ${formattedX}, ${pt.y.toFixed(2)} ⭐`;
      });
    }

    // -- Fetch Logistics Data --
    const logData = await fetchJSON('/api/logistics_data');
    if (logData) {
      // 1. Update KPIs
      if (logData.kpis) {
        document.getElementById('kpiAvgDelivery').innerText = logData.kpis.avg_delivery_days + " " + (window.LANG_MODE === 'EN' ? "days" : "ngày");
        document.getElementById('kpiLateRate').innerText = logData.kpis.late_rate + "%";
        document.getElementById('kpiFreightRatio').innerText = logData.kpis.avg_freight_ratio + "%";
      }

      // 2. Status Chart (Doughnut)
      if (logData.status_distribution) {
        ChartTheme.createDoughnut(
          'statusChart',
          logData.status_distribution.labels,
          logData.status_distribution.data
        );
      }

      // 3. State Delivery Time (Bar Chart)
      if (logData.state_delivery) {
        ChartTheme.createBarChart(
          'stateDeliveryChart',
          logData.state_delivery.labels,
          logData.state_delivery.data,
          window.LANG_MODE === 'EN' ? 'Avg Delivery Days' : 'Ngày giao hàng trung bình',
          ChartTheme.COLORS.indigo,
          (ctx) => {
            const stateCode = ctx.label;
            const fullName = getFullStateName(stateCode);
            const val = ctx.raw;
            return `${fullName} (${stateCode}): ${window.LANG_MODE === 'EN' ? 'Avg' : 'TB'} ${val} ${window.LANG_MODE === 'EN' ? 'days' : 'ngày'}`;
          }
        );
      }

      // 4. Impact on Review (Bar Chart)
      if (logData.impact) {
        ChartTheme.createBarChart(
          'impactChart',
          logData.impact.labels,
          logData.impact.data,
          window.LANG_MODE === 'EN' ? 'Avg Review Score' : 'Điểm đánh giá TB',
          ChartTheme.COLORS.pink
        );
      }
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
        window.CURRENCY_MODE === 'VND' ? 'Doanh Thu (VNĐ)' : 'Doanh Thu (BRL)',
        null,
        (ctx) => {
          const stateCode = ctx.chart.data.labels[ctx.dataIndex];
          const fullName = getFullStateName(stateCode);
          const yVal = formatCurrency(ctx.raw);
          return `${fullName}: Doanh thu ${yVal}`;
        }
      );
    }

    // Payment pie
    if (geo.payments && geo.payments.length) {
      ChartTheme.createPie(
        'paymentPie',
        geo.payments.map(d => translatePayment(d.method)),
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
      // PySpark dayofweek: 1=Sunday, 2=Monday, ..., 7=Saturday
      // HTML columns: 0=Monday (T2), ..., 6=Sunday (CN)
      let sparkDay = d.day;
      let mappedDay = sparkDay === 1 ? 6 : sparkDay - 2;
      
      const key = `${d.hour}-${mappedDay}`;
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
    // Update currency labels on the models page
    const curLabels = document.querySelectorAll('.currency-label-models');
    curLabels.forEach(el => {
      el.textContent = window.CURRENCY_MODE === 'VND' ? 'VND' : 'BRL';
    });

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

    const clsModels = models.filter(m => !m.is_regression);

    // Metrics table (Classification)
    const tbody = document.getElementById('metricsBody');
    if (tbody) {
      const fmt = (v) => (v == null || isNaN(v)) ? '—' : (v * 100).toFixed(1) + '%';
      tbody.innerHTML = clsModels.map(m => `
        <tr>
          <td style="color:var(--text-bright);font-weight:600">${m.name || '—'}</td>
          <td>${fmt(m.accuracy)}</td>
          <td>${fmt(m.precision)}</td>
          <td>${fmt(m.recall)}</td>
          <td>${fmt(m.f1)}</td>
          <td>${fmt(m.auc)}</td>
        </tr>
      `).join('') || `<tr><td colspan="6" style="text-align:center">Không có dữ liệu</td></tr>`;
    }

    // Prediction form
    const form = document.getElementById('predictForm');
    if (form) {
      form.addEventListener('submit', handlePredict);
    }

    
    const inpCustIdChurn = document.getElementById('inp_customer_id_churn');
    if (inpCustIdChurn) {
      inpCustIdChurn.addEventListener('input', (e) => {
        if (e.target.value.trim().length === 32) {
          handleAutofillChurn();
        }
      });
    }
  }

  async function handleAutofillChurn() {
    const custId = document.getElementById('inp_customer_id_churn').value.trim();
    if (!custId) {
      alert("Vui lòng nhập Mã Khách Hàng (Customer Unique ID)");
      return;
    }

    try {
      const res = await fetch(`/api/customers/${custId}`);
      if (!res.ok) {
        throw new Error("Không tìm thấy khách hàng này");
      }
      const data = await res.json();
      
      // Fill the fields
      document.getElementById('inp_recency').value = Math.round(data.recency);
      document.getElementById('inp_frequency').value = Math.round(data.frequency);
      document.getElementById('inp_monetary').value = convertCur(data.monetary).toFixed(0);
      document.getElementById('inp_review').value = data.review_score.toFixed(1);
      document.getElementById('inp_delivery').value = Math.round(data.delivery_days);
      
    } catch (err) {
      alert(err.message || "Đã xảy ra lỗi khi tải dữ liệu");
    }
  }

  async function handlePredict(e) {
    e.preventDefault();
    const form = e.target;
    
    let monetaryVal = parseFloat(form.monetary.value) || 0;
    if (window.CURRENCY_MODE === 'VND') {
      monetaryVal = monetaryVal / VND_RATE;
    }

    const body = {
      customer_id:   document.getElementById('inp_customer_id_churn').value.trim(),
      recency:       parseFloat(form.recency.value) || 0,
      frequency:     parseFloat(form.frequency.value) || 1,
      monetary:      monetaryVal,
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
      const pct = prob < 1 ? prob * 100 : prob;
      if (icon) icon.textContent = prob >= 0.5 ? '🚨' : '✅';
      label.textContent = data.label || '—';
      probEl.textContent = pct.toFixed(1) + '%';
      riskEl.textContent = data.risk_level || '—';

      if (prob >= 0.5) resultDiv.classList.add('result-high');
      else if (prob >= 0.3) resultDiv.classList.add('result-medium');
      else resultDiv.classList.add('result-low');

      setTimeout(() => {
        bar.style.width = pct + '%';
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
