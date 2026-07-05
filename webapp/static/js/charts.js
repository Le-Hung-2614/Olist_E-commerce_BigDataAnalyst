/* ═══════════════════════════════════════════════════════════════════
   Olist Dashboard — Chart.js Theme & Helpers
   ═══════════════════════════════════════════════════════════════════ */

const ChartTheme = (() => {
  // ── Color Palettes ──────────────────────────────────────────────
  const COLORS = {
    purple:  '#8b5cf6',
    blue:    '#3b82f6',
    cyan:    '#06b6d4',
    green:   '#10b981',
    orange:  '#f59e0b',
    red:     '#ef4444',
    pink:    '#ec4899',
    indigo:  '#6366f1',
    teal:    '#14b8a6',
    amber:   '#f59e0b',
    lime:    '#84cc16',
    rose:    '#f43f5e',
  };

  const PALETTE = [
    COLORS.purple, COLORS.cyan, COLORS.orange, COLORS.green,
    COLORS.blue, COLORS.pink, COLORS.indigo, COLORS.teal,
    COLORS.red, COLORS.amber, COLORS.lime, COLORS.rose
  ];

  const GRADIENTS = {
    purpleBlue: (ctx) => {
      const g = ctx.createLinearGradient(0, 0, 0, ctx.canvas.height);
      g.addColorStop(0, 'rgba(139, 92, 246, 0.45)');
      g.addColorStop(1, 'rgba(59, 130, 246, 0.02)');
      return g;
    },
    cyanTeal: (ctx) => {
      const g = ctx.createLinearGradient(0, 0, 0, ctx.canvas.height);
      g.addColorStop(0, 'rgba(6, 182, 212, 0.45)');
      g.addColorStop(1, 'rgba(20, 184, 166, 0.02)');
      return g;
    },
    orangeRed: (ctx) => {
      const g = ctx.createLinearGradient(0, 0, 0, ctx.canvas.height);
      g.addColorStop(0, 'rgba(245, 158, 11, 0.45)');
      g.addColorStop(1, 'rgba(239, 68, 68, 0.02)');
      return g;
    },
  };

  // ── Shared Defaults ─────────────────────────────────────────────
  const FONT_FAMILY = "'Inter', sans-serif";

  const DEFAULT_OPTIONS = {
    responsive: true,
    maintainAspectRatio: false,
    animation: {
      duration: 1200,
      easing: 'easeOutQuart',
    },
    plugins: {
      legend: {
        labels: {
          color: '#94a3b8',
          font: { family: FONT_FAMILY, size: 11, weight: '500' },
          padding: 16,
          usePointStyle: true,
          pointStyleWidth: 10,
        },
      },
      tooltip: {
        backgroundColor: 'rgba(10, 14, 39, 0.92)',
        titleColor: '#f8fafc',
        bodyColor: '#e2e8f0',
        borderColor: 'rgba(139, 92, 246, 0.25)',
        borderWidth: 1,
        cornerRadius: 8,
        padding: 12,
        titleFont: { family: FONT_FAMILY, size: 12, weight: '700' },
        bodyFont: { family: FONT_FAMILY, size: 11, weight: '400' },
        displayColors: true,
        boxPadding: 4,
      },
    },
    scales: {
      x: {
        grid: { color: 'rgba(148, 163, 184, 0.06)', drawBorder: false },
        ticks: {
          color: '#64748b',
          font: { family: FONT_FAMILY, size: 10, weight: '500' },
          padding: 8,
        },
        border: { display: false },
      },
      y: {
        grid: { color: 'rgba(148, 163, 184, 0.06)', drawBorder: false },
        ticks: {
          color: '#64748b',
          font: { family: FONT_FAMILY, size: 10, weight: '500' },
          padding: 8,
        },
        border: { display: false },
      },
    },
  };

  // ── Factory Functions ───────────────────────────────────────────

  /**
   * Line chart (e.g., revenue trend)
   */
  function createLineChart(canvasId, labels, data, label = 'Revenue') {
    const ctx = document.getElementById(canvasId);
    if (!ctx) return null;
    const context = ctx.getContext('2d');

    return new Chart(ctx, {
      type: 'line',
      data: {
        labels,
        datasets: [{
          label,
          data,
          borderColor: COLORS.purple,
          backgroundColor: GRADIENTS.purpleBlue(context),
          borderWidth: 2.5,
          fill: true,
          tension: 0.4,
          pointRadius: 3,
          pointHoverRadius: 6,
          pointBackgroundColor: COLORS.purple,
          pointBorderColor: '#0a0e27',
          pointBorderWidth: 2,
          pointHoverBackgroundColor: '#fff',
        }],
      },
      options: deepMerge({}, DEFAULT_OPTIONS, {
        plugins: {
          legend: { display: false },
          tooltip: {
            callbacks: {
              label: (c) => `${c.dataset.label}: ${formatNumber(c.raw)} BRL`,
            },
          },
        },
        scales: {
          y: {
            ticks: {
              callback: (v) => formatCompact(v) + ' BRL',
            },
          },
        },
      }),
    });
  }

  /**
   * Bar chart (vertical)
   */
  function createBarChart(canvasId, labels, data, label = 'Value', color = null) {
    const ctx = document.getElementById(canvasId);
    if (!ctx) return null;

    const colors = color
      ? Array(labels.length).fill(color)
      : labels.map((_, i) => withAlpha(PALETTE[i % PALETTE.length], 0.75));

    const borderColors = color
      ? Array(labels.length).fill(color)
      : labels.map((_, i) => PALETTE[i % PALETTE.length]);

    return new Chart(ctx, {
      type: 'bar',
      data: {
        labels,
        datasets: [{
          label,
          data,
          backgroundColor: colors,
          borderColor: borderColors,
          borderWidth: 1,
          borderRadius: 6,
          borderSkipped: false,
        }],
      },
      options: deepMerge({}, DEFAULT_OPTIONS, {
        plugins: { legend: { display: false } },
        scales: {
          y: {
            beginAtZero: true,
            ticks: { callback: (v) => formatCompact(v) },
          },
        },
      }),
    });
  }

  /**
   * Horizontal bar chart
   */
  function createHorizontalBar(canvasId, labels, data, label = 'Value') {
    const ctx = document.getElementById(canvasId);
    if (!ctx) return null;

    return new Chart(ctx, {
      type: 'bar',
      data: {
        labels,
        datasets: [{
          label,
          data,
          backgroundColor: labels.map((_, i) => withAlpha(PALETTE[i % PALETTE.length], 0.7)),
          borderColor: labels.map((_, i) => PALETTE[i % PALETTE.length]),
          borderWidth: 1,
          borderRadius: 6,
          borderSkipped: false,
        }],
      },
      options: deepMerge({}, DEFAULT_OPTIONS, {
        indexAxis: 'y',
        plugins: { legend: { display: false } },
        scales: {
          x: {
            beginAtZero: true,
            ticks: { callback: (v) => formatCompact(v) },
          },
        },
      }),
    });
  }

  /**
   * Doughnut chart
   */
  function createDoughnut(canvasId, labels, data) {
    const ctx = document.getElementById(canvasId);
    if (!ctx) return null;

    return new Chart(ctx, {
      type: 'doughnut',
      data: {
        labels,
        datasets: [{
          data,
          backgroundColor: labels.map((_, i) => withAlpha(PALETTE[i % PALETTE.length], 0.8)),
          borderColor: '#0a0e27',
          borderWidth: 3,
          hoverOffset: 14,
        }],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        cutout: '65%',
        animation: { animateRotate: true, duration: 1400, easing: 'easeOutQuart' },
        plugins: {
          legend: {
            position: 'bottom',
            labels: {
              color: '#94a3b8',
              font: { family: FONT_FAMILY, size: 11, weight: '500' },
              padding: 16,
              usePointStyle: true,
              pointStyleWidth: 10,
            },
          },
          tooltip: DEFAULT_OPTIONS.plugins.tooltip,
        },
      },
    });
  }

  /**
   * Pie chart
   */
  function createPie(canvasId, labels, data) {
    const ctx = document.getElementById(canvasId);
    if (!ctx) return null;

    return new Chart(ctx, {
      type: 'pie',
      data: {
        labels,
        datasets: [{
          data,
          backgroundColor: labels.map((_, i) => withAlpha(PALETTE[i % PALETTE.length], 0.8)),
          borderColor: '#0a0e27',
          borderWidth: 3,
          hoverOffset: 12,
        }],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: { animateRotate: true, duration: 1400, easing: 'easeOutQuart' },
        plugins: {
          legend: {
            position: 'bottom',
            labels: {
              color: '#94a3b8',
              font: { family: FONT_FAMILY, size: 11, weight: '500' },
              padding: 16,
              usePointStyle: true,
            },
          },
          tooltip: DEFAULT_OPTIONS.plugins.tooltip,
        },
      },
    });
  }

  /**
   * Scatter chart
   */
  function createScatter(canvasId, datasets, xLabel = 'X', yLabel = 'Y') {
    const ctx = document.getElementById(canvasId);
    if (!ctx) return null;

    return new Chart(ctx, {
      type: 'scatter',
      data: { datasets },
      options: deepMerge({}, DEFAULT_OPTIONS, {
        plugins: {
          legend: {
            position: 'top',
          },
        },
        scales: {
          x: {
            title: {
              display: true, text: xLabel,
              color: '#94a3b8',
              font: { family: FONT_FAMILY, size: 11, weight: '600' },
            },
          },
          y: {
            title: {
              display: true, text: yLabel,
              color: '#94a3b8',
              font: { family: FONT_FAMILY, size: 11, weight: '600' },
            },
          },
        },
      }),
    });
  }

  /**
   * Radar chart
   */
  function createRadar(canvasId, labels, datasets) {
    const ctx = document.getElementById(canvasId);
    if (!ctx) return null;

    return new Chart(ctx, {
      type: 'radar',
      data: { labels, datasets },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: { duration: 1200, easing: 'easeOutQuart' },
        scales: {
          r: {
            grid: { color: 'rgba(148, 163, 184, 0.08)' },
            angleLines: { color: 'rgba(148, 163, 184, 0.08)' },
            ticks: { display: false },
            pointLabels: {
              color: '#94a3b8',
              font: { family: FONT_FAMILY, size: 10, weight: '500' },
            },
          },
        },
        plugins: {
          legend: {
            position: 'bottom',
            labels: {
              color: '#94a3b8',
              font: { family: FONT_FAMILY, size: 11, weight: '500' },
              padding: 16,
              usePointStyle: true,
            },
          },
          tooltip: DEFAULT_OPTIONS.plugins.tooltip,
        },
      },
    });
  }

  /**
   * Grouped / multi-dataset bar chart
   */
  function createGroupedBar(canvasId, labels, datasets) {
    const ctx = document.getElementById(canvasId);
    if (!ctx) return null;

    return new Chart(ctx, {
      type: 'bar',
      data: { labels, datasets },
      options: deepMerge({}, DEFAULT_OPTIONS, {
        plugins: {
          legend: { position: 'top' },
        },
        scales: {
          y: {
            beginAtZero: true,
            max: 1,
            ticks: { callback: (v) => (v * 100).toFixed(0) + '%' },
          },
        },
      }),
    });
  }

  // ── Utility ─────────────────────────────────────────────────────

  function withAlpha(hex, alpha) {
    const r = parseInt(hex.slice(1, 3), 16);
    const g = parseInt(hex.slice(3, 5), 16);
    const b = parseInt(hex.slice(5, 7), 16);
    return `rgba(${r}, ${g}, ${b}, ${alpha})`;
  }

  function formatNumber(n) {
    if (n == null) return '0';
    return Number(n).toLocaleString('vi-VN', { minimumFractionDigits: 0, maximumFractionDigits: 2 });
  }

  function formatCompact(n) {
    if (n >= 1e6) return (n / 1e6).toFixed(1) + 'M';
    if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K';
    return n.toString();
  }

  function deepMerge(target, ...sources) {
    for (const src of sources) {
      for (const key of Object.keys(src)) {
        if (src[key] && typeof src[key] === 'object' && !Array.isArray(src[key])) {
          if (!target[key]) target[key] = {};
          deepMerge(target[key], src[key]);
        } else {
          target[key] = src[key];
        }
      }
    }
    return target;
  }

  // ── Public API ──────────────────────────────────────────────────
  return {
    COLORS, PALETTE, GRADIENTS,
    createLineChart, createBarChart, createHorizontalBar,
    createDoughnut, createPie, createScatter, createRadar,
    createGroupedBar,
    withAlpha, formatNumber, formatCompact,
  };
})();
