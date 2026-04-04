/* charts.js — data loading, chart rendering, table building */
(function () {
  'use strict';

  var instances = [];

  /* ================================================================
     Regime bands drawn behind time-series charts
     ================================================================ */
  var BANDS = [
    { s: '2008-11-25', e: '2014-10-29', label: 'QE',    k: 'regimeQE' },
    { s: '2017-10-01', e: '2019-07-31', label: 'QT1',   k: 'regimeQT' },
    { s: '2019-08-01', e: '2022-05-31', label: 'QE',    k: 'regimeQE' },
    { s: '2022-06-01', e: '2025-03-31', label: 'QT2',   k: 'regimeQT' },
    { s: '2025-04-01', e: '2026-12-31', label: 'Taper', k: 'regimeTaper' }
  ];

  var regimePlugin = {
    id: 'regimeShading',
    beforeDraw: function (chart) {
      if (!chart.options.plugins.regimeShading) return;
      var ctx = chart.ctx, area = chart.chartArea, xs = chart.scales.x;
      if (!area || !xs) return;
      var c = cwColors();
      BANDS.forEach(function (b) {
        var x1 = xs.getPixelForValue(new Date(b.s).getTime());
        var x2 = xs.getPixelForValue(new Date(b.e).getTime());
        var left = Math.max(x1, area.left), right = Math.min(x2, area.right);
        if (left >= right) return;
        ctx.save();
        ctx.fillStyle = c[b.k];
        ctx.fillRect(left, area.top, right - left, area.bottom - area.top);
        ctx.fillStyle = c.textMuted;
        ctx.font = '10px system-ui, sans-serif';
        ctx.textAlign = 'center';
        ctx.globalAlpha = 0.6;
        ctx.fillText(b.label, (left + right) / 2, area.top + 13);
        ctx.restore();
      });
    }
  };
  Chart.register(regimePlugin);

  /* ================================================================
     Data loading
     ================================================================ */
  function fetchJ(name) {
    return fetch('data/' + name + '.json').then(function (r) {
      if (!r.ok) throw new Error(name + ': ' + r.status);
      return r.json();
    });
  }

  function loadAll() {
    return Promise.all([
      fetchJ('weekly_panel'),
      fetchJ('quarterly_descriptive'),
      fetchJ('regime_summary'),
      fetchJ('qt_comparison_summary'),
      fetchJ('episode_summary'),
      fetchJ('correlation_matrix'),
      fetchJ('reaction_function_main'),
      fetchJ('main_lp_dealer'),
      fetchJ('main_lp_repo'),
      fetchJ('summary'),
      fetchJ('manual_input_audit'),
      fetchJ('daily_mechanics_appendix'),
      fetchJ('daily_validation_appendix'),
      fetchJ('sectoral_absorbers_appendix'),
      fetchJ('auction_mix_appendix')
    ]).then(function (arr) {
      return {
        weekly: arr[0], qDesc: arr[1], regime: arr[2],
        qtCompare: arr[3], episodes: arr[4], corr: arr[5], reaction: arr[6],
        lpDealer: arr[7], lpRepo: arr[8], summary: arr[9], manualAudit: arr[10], dailyAppendix: arr[11], dailyValidation: arr[12], sectoralAppendix: arr[13], auctionMixAppendix: arr[14]
      };
    });
  }

  /* ================================================================
     Helpers
     ================================================================ */
  function fmt(v, dec) {
    if (v == null || isNaN(v)) return '\u2014';
    return Number(v).toLocaleString('en-US', { minimumFractionDigits: dec || 0, maximumFractionDigits: dec || 0 });
  }

  function fmtBn(v) {
    if (v == null || isNaN(v)) return '\u2014';
    var n = Number(v);
    if (Math.abs(n) >= 1000) return '$' + (n / 1000).toFixed(1) + 'T';
    return '$' + Math.round(n) + 'B';
  }

  function fmtCorr(v) {
    if (v == null || isNaN(v)) return '\u2014';
    var n = Number(v);
    return (n < 0 ? '&minus;' : '') + Math.abs(n).toFixed(2);
  }

  function el(id) { return document.getElementById(id); }

  /* ================================================================
     Chart defaults (applied per-build, theme-aware)
     ================================================================ */
  function applyDefaults() {
    var c = cwColors();
    Chart.defaults.color = c.text;
    Chart.defaults.borderColor = c.grid;
    Chart.defaults.font.family = "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif";
    Chart.defaults.font.size = 12;
    Chart.defaults.plugins.tooltip.backgroundColor = c.tooltipBg;
    Chart.defaults.plugins.tooltip.titleColor = c.tooltipTitle;
    Chart.defaults.plugins.tooltip.bodyColor = c.tooltipBody;
    Chart.defaults.plugins.tooltip.cornerRadius = 6;
    Chart.defaults.plugins.tooltip.padding = 10;
    Chart.defaults.plugins.tooltip.displayColors = true;
    Chart.defaults.plugins.legend.labels.usePointStyle = true;
  }

  /* ================================================================
     Shared time-series options
     ================================================================ */
  function tsOpts(title, yLabel, yCallback) {
    var c = cwColors();
    return {
      responsive: true,
      maintainAspectRatio: true,
      aspectRatio: 2,
      animation: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        regimeShading: true,
        legend: { display: false },
        title: { display: true, text: title, color: c.text, font: { size: 14, weight: '600' }, padding: { bottom: 12 } },
        tooltip: { callbacks: { label: function (ctx) { return ctx.dataset.label + ': ' + (yCallback ? yCallback(ctx.parsed.y) : fmt(ctx.parsed.y, 1)); } } }
      },
      scales: {
        x: { type: 'time', time: { unit: 'year', tooltipFormat: 'MMM d, yyyy' }, grid: { display: false }, ticks: { color: c.textMuted, maxTicksLimit: 10 } },
        y: { title: { display: true, text: yLabel, color: c.textMuted }, grid: { color: c.grid }, ticks: { color: c.textMuted, callback: yCallback } }
      }
    };
  }

  function tsDataset(label, data, color, extra) {
    var ds = {
      label: label,
      data: data,
      borderColor: color,
      backgroundColor: color + '18',
      borderWidth: 2,
      pointRadius: 0,
      pointHoverRadius: 4,
      pointHoverBackgroundColor: color,
      tension: 0,
      fill: false
    };
    if (extra) Object.assign(ds, extra);
    return ds;
  }

  /* ================================================================
     Individual chart builders
     ================================================================ */

  function buildSoma(weekly) {
    var c = cwColors();
    var data = weekly.filter(function (r) { return r.soma_treasuries_bn != null; })
      .map(function (r) { return { x: r.week, y: r.soma_treasuries_bn }; });
    instances.push(new Chart(el('somaChart'), {
      type: 'line',
      data: { datasets: [tsDataset('SOMA Treasury Holdings', data, c.teal, { fill: true })] },
      options: tsOpts('SOMA Treasury Holdings', '$ billions', fmtBn)
    }));
  }

  function buildOnRrp(weekly) {
    var c = cwColors();
    var data = weekly.filter(function (r) { return r.on_rrp_bn != null && r.week >= '2014-01-01'; })
      .map(function (r) { return { x: r.week, y: r.on_rrp_bn }; });
    instances.push(new Chart(el('onrrpChart'), {
      type: 'line',
      data: { datasets: [tsDataset('ON RRP Balance', data, c.blue, { fill: true })] },
      options: tsOpts('ON RRP Balance', '$ billions', fmtBn)
    }));
  }

  function buildDealer(weekly) {
    var c = cwColors();
    var data = weekly.filter(function (r) { return r.dealer_inventory_bn != null; })
      .map(function (r) { return { x: r.week, y: r.dealer_inventory_bn }; });
    instances.push(new Chart(el('dealerChart'), {
      type: 'line',
      data: { datasets: [tsDataset('Dealer Treasury Inventories', data, c.amber, { fill: true })] },
      options: tsOpts('Primary Dealer Treasury Inventories', '$ billions', fmtBn)
    }));
  }

  function buildRepo(weekly) {
    var c = cwColors();
    var data = weekly.filter(function (r) { return r.repo_spread_bp != null; })
      .map(function (r) { return { x: r.week, y: r.repo_spread_bp }; });
    var opts = tsOpts('Repo Spread (TGCR \u2212 ON RRP rate)', 'basis points', function (v) { return fmt(v, 0) + ' bp'; });
    // Add zero reference line
    opts.plugins.annotation = undefined; // Chart.js annotation not loaded; we'll draw it in plugin
    instances.push(new Chart(el('repoChart'), {
      type: 'line',
      data: { datasets: [tsDataset('Repo Spread', data, c.red)] },
      options: opts,
      plugins: [{
        id: 'zeroLine',
        beforeDraw: function (chart) {
          var ys = chart.scales.y, area = chart.chartArea;
          if (!ys || !area) return;
          var y = ys.getPixelForValue(0);
          if (y < area.top || y > area.bottom) return;
          var ctx = chart.ctx;
          ctx.save();
          ctx.strokeStyle = cwColors().textMuted;
          ctx.lineWidth = 1;
          ctx.setLineDash([4, 4]);
          ctx.beginPath();
          ctx.moveTo(area.left, y);
          ctx.lineTo(area.right, y);
          ctx.stroke();
          ctx.restore();
        }
      }]
    }));
  }

  function buildNetDuration(qDesc) {
    var c = cwColors();
    var qt = qDesc.filter(function (r) { return r.quarter >= '2021Q1'; });
    var labels = qt.map(function (r) { return r.quarter; });
    var net = qt.map(function (r) { return r.net_private_duration_dv01; });
    var buyback = qt.map(function (r) { return -(r.buyback_offset_dv01 || 0); });

    var barColors = net.map(function (v) { return v < 0 ? c.green : c.amber; });

    instances.push(new Chart(el('netDurationChart'), {
      type: 'bar',
      data: {
        labels: labels,
        datasets: [
          { label: 'Net Private Duration Supply (DV01)', data: net, backgroundColor: barColors, borderRadius: 3 },
          { label: 'Buyback Removal (DV01)', data: buyback, backgroundColor: c.purple + '80', borderRadius: 3 }
        ]
      },
      options: {
        responsive: true, maintainAspectRatio: true, aspectRatio: 2.2,
        animation: false,
        plugins: {
          title: { display: true, text: 'Net Private Duration Supply', color: c.text, font: { size: 14, weight: '600' }, padding: { bottom: 12 } },
          legend: { position: 'bottom', labels: { color: c.textMuted, usePointStyle: true, padding: 16 } },
          tooltip: { callbacks: { label: function (ctx) { return ctx.dataset.label + ': ' + fmt(ctx.parsed.y, 0) + ' DV01'; } } }
        },
        scales: {
          x: { grid: { display: false }, ticks: { color: c.textMuted, maxRotation: 45, font: { size: 10 } } },
          y: { title: { display: true, text: 'DV01', color: c.textMuted }, grid: { color: c.grid }, ticks: { color: c.textMuted } }
        }
      },
      plugins: [{
        id: 'zeroLine',
        beforeDraw: function (chart) {
          var ys = chart.scales.y, area = chart.chartArea;
          if (!ys || !area) return;
          var y = ys.getPixelForValue(0);
          if (y < area.top || y > area.bottom) return;
          var ctx = chart.ctx;
          ctx.save();
          ctx.strokeStyle = cwColors().textMuted;
          ctx.lineWidth = 1;
          ctx.setLineDash([4, 4]);
          ctx.beginPath(); ctx.moveTo(area.left, y); ctx.lineTo(area.right, y); ctx.stroke();
          ctx.restore();
        }
      }]
    }));
  }

  function buildOnRrpShare(qDesc) {
    var c = cwColors();
    var qt = qDesc.filter(function (r) { return r.quarter >= '2021Q1' && r.on_rrp_share != null; });
    var labels = qt.map(function (r) { return r.quarter; });
    var data = qt.map(function (r) { return (r.on_rrp_share * 100); });

    instances.push(new Chart(el('onrrpShareChart'), {
      type: 'line',
      data: {
        labels: labels,
        datasets: [{
          label: 'ON RRP as % of System Liquidity',
          data: data,
          borderColor: c.blue,
          backgroundColor: c.blue + '18',
          borderWidth: 2, pointRadius: 4, pointHoverRadius: 6,
          pointBackgroundColor: c.blue, fill: true, tension: 0.2
        }]
      },
      options: {
        responsive: true, maintainAspectRatio: true, aspectRatio: 1.4,
        animation: false,
        plugins: {
          legend: { display: false },
          title: { display: true, text: 'ON RRP Share of System Liquidity', color: c.text, font: { size: 14, weight: '600' }, padding: { bottom: 12 } },
          tooltip: { callbacks: { label: function (ctx) { return fmt(ctx.parsed.y, 1) + '%'; } } }
        },
        scales: {
          x: { grid: { display: false }, ticks: { color: c.textMuted, maxRotation: 45, font: { size: 10 } } },
          y: { title: { display: true, text: '% of reserves + ON RRP', color: c.textMuted }, grid: { color: c.grid }, ticks: { color: c.textMuted, callback: function (v) { return v + '%'; } }, min: 0 }
        }
      }
    }));
  }

  function buildTga(weekly) {
    var c = cwColors();
    var qt2 = weekly.filter(function (r) {
      return r.week >= '2022-06-01' && r.tga_bn != null && r.reserves_bn != null;
    });
    var data = qt2.map(function (r) { return { x: r.tga_bn, y: r.reserves_bn }; });

    instances.push(new Chart(el('tgaChart'), {
      type: 'scatter',
      data: {
        datasets: [{
          label: 'TGA vs Reserves (QT2)',
          data: data,
          backgroundColor: c.teal + '60',
          borderColor: c.teal,
          borderWidth: 1, pointRadius: 2.5, pointHoverRadius: 5
        }]
      },
      options: {
        responsive: true, maintainAspectRatio: true, aspectRatio: 1.4,
        animation: false,
        plugins: {
          legend: { display: false },
          title: { display: true, text: 'TGA vs Reserves (QT2 period)', color: c.text, font: { size: 14, weight: '600' }, padding: { bottom: 12 } },
          tooltip: { callbacks: { label: function (ctx) { return 'TGA: ' + fmtBn(ctx.parsed.x) + '  Reserves: ' + fmtBn(ctx.parsed.y); } } }
        },
        scales: {
          x: { title: { display: true, text: 'TGA ($ billions)', color: c.textMuted }, grid: { color: c.grid }, ticks: { color: c.textMuted, callback: function (v) { return '$' + v + 'B'; } } },
          y: { title: { display: true, text: 'Reserves ($ billions)', color: c.textMuted }, grid: { color: c.grid }, ticks: { color: c.textMuted, callback: function (v) { return '$' + v + 'B'; } } }
        }
      }
    }));
  }

  function pctChange(current, start) {
    if (current == null || start == null || isNaN(current) || isNaN(start) || Number(start) === 0) return '\u2014';
    var pct = ((Number(current) / Number(start)) - 1) * 100;
    var sign = pct > 0 ? '+' : '';
    return sign + fmt(pct, 1) + '%';
  }

  function fmtSignedBn(v) {
    if (v == null || isNaN(v)) return '\u2014';
    var n = Number(v);
    var sign = n > 0 ? '+' : '';
    return sign + fmt(n, 1) + 'B';
  }

  function fmtPct(v) {
    if (v == null || isNaN(v)) return '\u2014';
    return fmt(Number(v) * 100, 1) + '%';
  }

  function fmtSignedPp(v) {
    if (v == null || isNaN(v)) return '\u2014';
    var n = Number(v);
    var sign = n > 0 ? '+' : '';
    return sign + fmt(n, 1) + ' pp';
  }

  function fmtSignedAmt(v) {
    if (v == null || isNaN(v)) return '\u2014';
    var n = Number(v);
    var sign = n > 0 ? '+' : '';
    var abs = Math.abs(n);
    var shown = abs >= 1000 ? fmtBn(abs) : ('$' + fmt(abs, 1) + 'B');
    return sign + shown;
  }

  function med(arr) {
    if (!arr || !arr.length) return null;
    var vals = arr.slice().filter(function (v) { return v != null && !isNaN(v); }).sort(function (a, b) { return a - b; });
    if (!vals.length) return null;
    var mid = Math.floor(vals.length / 2);
    return vals.length % 2 ? vals[mid] : (vals[mid - 1] + vals[mid]) / 2;
  }

  function buildChannelDashboard(weekly) {
    var cardsTarget = el('channelCards');
    var splitTarget = el('bufferSplitTable');
    var insightTarget = el('channelInsight');
    if (!cardsTarget || !splitTarget || !insightTarget || !weekly || !weekly.length) return;

    var qt2 = weekly.filter(function (r) {
      return r.week >= '2022-06-01' && r.system_liquidity_bn != null;
    });
    if (!qt2.length) return;

    var latest = qt2[qt2.length - 1];
    var start = qt2[0];
    var withDealers = qt2.filter(function (r) { return r.dealer_inventory_bn != null; });
    var recent13 = qt2.slice(Math.max(qt2.length - 13, 0));
    var cumulativeRunoff = qt2.reduce(function (sum, row) {
      return sum + (Number(row.qt_runoff_dv01) || 0);
    }, 0);
    var reserveCards = [
      ['System liquidity', fmtBn(latest.system_liquidity_bn)],
      ['Change since QT2 start', pctChange(latest.system_liquidity_bn, start.system_liquidity_bn)],
      ['ON RRP latest', fmtBn(latest.on_rrp_bn)]
    ];
    var durationCards = [
      ['Duration pressure', fmt(latest.duration_pressure_dv01, 1) + ' DV01'],
      ['Bills offset', fmt(latest.bill_dv01_offset, 1) + ' DV01'],
      ['Cum QT runoff', fmt(cumulativeRunoff, 1) + ' DV01']
    ];
    var intermediationCards = [
      ['Dealer inventory', latest.dealer_inventory_bn != null ? fmtBn(latest.dealer_inventory_bn) : '\u2014'],
      ['Dealer change since QT2 start', withDealers.length ? pctChange(withDealers[withDealers.length - 1].dealer_inventory_bn, withDealers[0].dealer_inventory_bn) : '\u2014'],
      ['Repo spread, 13w median', med(recent13.map(function (r) { return r.repo_spread_bp; })) != null ? fmt(med(recent13.map(function (r) { return r.repo_spread_bp; })), 0) + ' bp' : '\u2014']
    ];

    function cardHtml(title, body, metrics) {
      return '<div class="channel-card">' +
        '<h3>' + title + '</h3>' +
        '<p>' + body + '</p>' +
        metrics.map(function (item) {
          return '<div class="channel-metric"><span>' + item[0] + '</span><strong>' + item[1] + '</strong></div>';
        }).join('') +
        '</div>';
    }

    cardsTarget.innerHTML =
      cardHtml('Reserve Channel', 'Reserves plus ON RRP capture the balance-sheet buffer available before drainage shows up more forcefully in markets.', reserveCards) +
      cardHtml('Duration Channel', 'QT runoff, coupon issuance, bills, and buybacks determine how much duration burden is pushed toward private holders.', durationCards) +
      cardHtml('Intermediation Channel', 'Dealer inventories and repo spreads indicate whether the system is absorbing that burden cheaply or at higher financing cost.', intermediationCards);

    var splitSample = qt2.filter(function (r) {
      return r.low_liquidity != null && r.system_liquidity_bn != null && r.fed_pressure_dv01 != null;
    });
    var splitMethodNote = 'Weekly medians split by the project&rsquo;s low-liquidity state. This keeps the emphasis on descriptive comparisons rather than interaction coefficients.';
    var lowStateCount = splitSample.filter(function (r) { return String(r.low_liquidity) === '1'; }).length;
    if (!lowStateCount) {
      var qt2Median = med(splitSample.map(function (r) { return Number(r.system_liquidity_bn); }));
      splitSample = splitSample.map(function (r) {
        var out = Object.assign({}, r);
        out.low_liquidity = Number(r.system_liquidity_bn) <= qt2Median ? 1 : 0;
        return out;
      });
      splitMethodNote = 'Weekly medians split at the QT2 sample median of system liquidity because the global low-liquidity flag does not separate the QT2 subsample.';
    }
    var splitRows = [
      { key: '0', label: 'Higher-buffer weeks' },
      { key: '1', label: 'Lower-buffer weeks' }
    ].map(function (spec) {
      var sub = splitSample.filter(function (r) { return String(r.low_liquidity) === spec.key; });
      return {
        state: spec.label,
        weeks: sub.length,
        median_system_liquidity_bn: med(sub.map(function (r) { return Number(r.system_liquidity_bn); })),
        median_fed_pressure_dv01: med(sub.map(function (r) { return Number(r.fed_pressure_dv01); })),
        median_dealer_inventory_bn: med(sub.map(function (r) { return r.dealer_inventory_bn == null ? null : Number(r.dealer_inventory_bn); })),
        median_repo_spread_bp: med(sub.map(function (r) { return r.repo_spread_bp == null ? null : Number(r.repo_spread_bp); }))
      };
    });

    splitTarget.innerHTML =
      '<h3>QT2 State Split</h3>' +
      '<p style="margin:0 0 12px;color:var(--c-text-muted);font-size:0.9rem">' + splitMethodNote + '</p>' +
      htmlTable(
        splitRows,
        ['state', 'weeks', 'median_system_liquidity_bn', 'median_fed_pressure_dv01', 'median_dealer_inventory_bn', 'median_repo_spread_bp'],
        {
          median_system_liquidity_bn: fmtBn,
          median_fed_pressure_dv01: function (v) { return fmt(v, 1) + ' DV01'; },
          median_dealer_inventory_bn: fmtBn,
          median_repo_spread_bp: function (v) { return fmt(v, 1) + ' bp'; }
        },
        {
          state: 'State',
          weeks: 'Weeks',
          median_system_liquidity_bn: 'Median Buffer',
          median_fed_pressure_dv01: 'Median Fed Pressure',
          median_dealer_inventory_bn: 'Median Dealers',
          median_repo_spread_bp: 'Median Repo Spread'
        }
      );

    var lower = splitRows[1];
    var higher = splitRows[0];
    insightTarget.innerHTML =
      '<strong>Channel readout:</strong> In lower-buffer QT2 weeks, median system liquidity is <strong>' + fmtBn(lower.median_system_liquidity_bn) +
      '</strong> versus <strong>' + fmtBn(higher.median_system_liquidity_bn) + '</strong> in higher-buffer weeks, while median repo spreads move to <strong>' +
      fmt(lower.median_repo_spread_bp, 1) + ' bp</strong> from <strong>' + fmt(higher.median_repo_spread_bp, 1) +
      ' bp</strong>. That keeps the comparison usable even when the full-sample liquidity flag does not divide the QT2 weeks on its own.';
  }

  function buildCashMechanicsAppendix(rows) {
    var chart2023 = el('cash2023Chart');
    var chart2025 = el('cash2025Chart');
    var summaryTarget = el('cashSummaryTable');
    var insightTarget = el('cashInsight');
    if (!chart2023 || !chart2025 || !summaryTarget || !insightTarget || !rows || !rows.length) return;

    function drawWindow(canvasEl, windowId, title) {
      var c = cwColors();
      var sub = rows.filter(function (r) { return r.window_id === windowId; });
      if (!sub.length) return null;
      instances.push(new Chart(canvasEl, {
        type: 'line',
        data: {
          datasets: [
            tsDataset('TGA', sub.map(function (r) { return { x: r.week, y: r.tga_bn }; }), c.red),
            tsDataset('Reserves', sub.map(function (r) { return { x: r.week, y: r.reserves_bn }; }), c.teal),
            tsDataset('ON RRP', sub.map(function (r) { return { x: r.week, y: r.on_rrp_bn }; }), c.blue)
          ]
        },
        options: tsOpts(title, '$ billions', fmtBn)
      }));
      return sub;
    }

    var win2023 = drawWindow(chart2023, 'debt_ceiling_2023', '2023 Debt-Ceiling Window');
    var win2025 = drawWindow(chart2025, 'debt_ceiling_2025', '2025 Debt-Ceiling Window');
    var windows = [
      { label: '2023 debt ceiling', rows: win2023 },
      { label: '2025 debt ceiling', rows: win2025 }
    ].filter(function (w) { return w.rows && w.rows.length; });

    var summaryRows = windows.map(function (w) {
      var first = w.rows[0];
      var last = w.rows[w.rows.length - 1];
      return {
        window: w.label,
        start_tga_bn: Number(first.tga_bn),
        end_tga_bn: Number(last.tga_bn),
        delta_tga_bn: Number(last.tga_bn) - Number(first.tga_bn),
        delta_reserves_bn: Number(last.reserves_bn) - Number(first.reserves_bn),
        delta_on_rrp_bn: Number(last.on_rrp_bn) - Number(first.on_rrp_bn)
      };
    });

    summaryTarget.innerHTML =
      '<h3>Debt-Ceiling Window Summary</h3>' +
      '<p style="margin:0 0 12px;color:var(--c-text-muted);font-size:0.9rem">Start-to-end changes over the published appendix windows. The point is mechanical separation, not causal proof.</p>' +
      htmlTable(
        summaryRows,
        ['window', 'start_tga_bn', 'end_tga_bn', 'delta_tga_bn', 'delta_reserves_bn', 'delta_on_rrp_bn'],
        {
          start_tga_bn: fmtBn,
          end_tga_bn: fmtBn,
          delta_tga_bn: fmtSignedBn,
          delta_reserves_bn: fmtSignedBn,
          delta_on_rrp_bn: fmtSignedBn
        },
        {
          window: 'Window',
          start_tga_bn: 'Start TGA',
          end_tga_bn: 'End TGA',
          delta_tga_bn: 'TGA Change',
          delta_reserves_bn: 'Reserves Change',
          delta_on_rrp_bn: 'ON RRP Change'
        }
      );

    if (summaryRows.length >= 2) {
      insightTarget.innerHTML =
        '<strong>Cash-mechanics readout:</strong> In the 2023 window, TGA moved <strong>' + fmtSignedBn(summaryRows[0].delta_tga_bn) +
        '</strong> while ON RRP moved <strong>' + fmtSignedBn(summaryRows[0].delta_on_rrp_bn) +
        '</strong>. In the 2025 window, TGA moved <strong>' + fmtSignedBn(summaryRows[1].delta_tga_bn) +
        '</strong> while ON RRP moved <strong>' + fmtSignedBn(summaryRows[1].delta_on_rrp_bn) +
        '</strong>. That is the appendix’s job: show when cash-balance shifts and money-market buffer shifts are doing work that should not be misread as pure QT reserve drain.';
    }
  }

  function buildCashValidationAppendix(validation) {
    var target = el('cashValidationTable');
    if (!target || !validation) return;
    if (!validation.summary || !validation.summary.length) {
      target.innerHTML =
        '<h3>DTS And Debt Cross-Check</h3>' +
        '<p style="margin:0 0 12px;color:var(--c-text-muted);font-size:0.9rem">' +
        ((validation.metadata && validation.metadata.notes && validation.metadata.notes[validation.metadata.notes.length - 1]) || 'Validation data unavailable.') +
        '</p>';
      return;
    }

    var rows = validation.summary.map(function (row) {
      return {
        window: row.window.replace(/_/g, ' '),
        matched_days: row.matched_days,
        mean_abs_gap_bn: row.mean_abs_gap_bn,
        max_abs_gap_bn: row.max_abs_gap_bn,
        end_gap_bn: row.end_gap_bn,
        debt_change_bn: row.debt_change_bn
      };
    });
    target.innerHTML =
      '<h3>DTS And Debt Cross-Check</h3>' +
      '<p style="margin:0 0 12px;color:var(--c-text-muted);font-size:0.9rem">This cross-check uses matching Wednesday observations where the H.4.1 TGA series and the Daily Treasury Statement can be compared directly. Non-zero gaps are expected because the two releases are not identical measures.</p>' +
      htmlTable(
        rows,
        ['window', 'matched_days', 'mean_abs_gap_bn', 'max_abs_gap_bn', 'end_gap_bn', 'debt_change_bn'],
        {
          mean_abs_gap_bn: fmtBn,
          max_abs_gap_bn: fmtBn,
          end_gap_bn: fmtSignedAmt,
          debt_change_bn: fmtSignedAmt
        },
        {
          window: 'Window',
          matched_days: 'Matched Wednesdays',
          mean_abs_gap_bn: 'Mean Abs TGA Gap',
          max_abs_gap_bn: 'Max Abs TGA Gap',
          end_gap_bn: 'End-Date Gap',
          debt_change_bn: 'Debt Change'
        }
      );
  }

  function buildSectoralAbsorbersAppendix(appendix) {
    var levelTarget = el('sectorLevelsChart');
    var shareTarget = el('sectorSharesChart');
    var tableTarget = el('sectorSummaryTable');
    var insightTarget = el('sectorInsight');
    if (!levelTarget || !shareTarget || !tableTarget || !insightTarget || !appendix || !appendix.series || !appendix.series.length) return;

    var c = cwColors();
    var series = appendix.series.filter(function (row) { return row.quarter >= '2009Q1'; });
    var modern = series.filter(function (row) { return row.quarter >= '2022Q2'; });
    var colorMap = {
      rest_of_world: c.blue,
      households_nonprofits: c.teal,
      us_chartered_depositories: c.amber,
      money_market_funds: c.green,
      mutual_funds: c.purple,
      broker_dealers: c.red,
      other_private_sectors: '#64748b'
    };
    var sectors = appendix.sectors || [];

    instances.push(new Chart(levelTarget, {
      type: 'line',
      data: {
        labels: series.map(function (row) { return row.quarter; }),
        datasets: sectors.map(function (sector) {
          return tsDataset(
            sector.label,
            series.map(function (row) { return row[sector.key]; }),
            colorMap[sector.key] || c.textMuted
          );
        })
      },
      options: {
        responsive: true,
        maintainAspectRatio: true,
        aspectRatio: 1.8,
        animation: false,
        plugins: {
          legend: { position: 'bottom', labels: { color: c.textMuted, usePointStyle: true, padding: 12 } },
          title: { display: true, text: 'Published Private Treasury Holders', color: c.text, font: { size: 14, weight: '600' }, padding: { bottom: 12 } },
          tooltip: { callbacks: { label: function (ctx) { return ctx.dataset.label + ': ' + fmtBn(ctx.parsed.y); } } }
        },
        scales: {
          x: { type: 'category', grid: { display: false }, ticks: { color: c.textMuted, maxTicksLimit: 8 } },
          y: { title: { display: true, text: '$ billions', color: c.textMuted }, grid: { color: c.grid }, ticks: { color: c.textMuted, callback: fmtBn } }
        }
      }
    }));

    instances.push(new Chart(shareTarget, {
      type: 'bar',
      data: {
        labels: modern.map(function (row) { return row.quarter; }),
        datasets: sectors.map(function (sector) {
          return {
            label: sector.label,
            data: modern.map(function (row) { return Number(row[sector.key + '_share']) * 100; }),
            backgroundColor: colorMap[sector.key] || c.textMuted,
            borderWidth: 0
          };
        })
      },
      options: {
        responsive: true,
        maintainAspectRatio: true,
        aspectRatio: 1.8,
        animation: false,
        plugins: {
          legend: { position: 'bottom', labels: { color: c.textMuted, usePointStyle: true, padding: 12 } },
          title: { display: true, text: 'Holder Shares Since QT2 Start', color: c.text, font: { size: 14, weight: '600' }, padding: { bottom: 12 } },
          tooltip: { callbacks: { label: function (ctx) { return ctx.dataset.label + ': ' + fmt(ctx.parsed.y, 1) + '%'; } } }
        },
        scales: {
          x: { stacked: true, grid: { display: false }, ticks: { color: c.textMuted, maxRotation: 45, font: { size: 10 } } },
          y: { stacked: true, title: { display: true, text: '% of published private holdings', color: c.textMuted }, grid: { color: c.grid }, ticks: { color: c.textMuted, callback: function (v) { return v + '%'; } }, max: 100 }
        }
      }
    }));

    tableTarget.innerHTML =
      '<h3>Latest Quarter Sector Summary</h3>' +
      '<p style="margin:0 0 12px;color:var(--c-text-muted);font-size:0.9rem">Quarter-end levels and changes since the QT2 start quarter in the appendix.</p>' +
      htmlTable(
        appendix.summary,
        ['sector', 'latest_level_bn', 'latest_share', 'change_since_qt2_bn'],
        {
          latest_level_bn: fmtBn,
          latest_share: fmtPct,
          change_since_qt2_bn: fmtSignedBn
        },
        {
          sector: 'Sector',
          latest_level_bn: 'Latest Level',
          latest_share: 'Latest Share',
          change_since_qt2_bn: 'Change Since QT2 Start'
        }
      );

    var latest = appendix.summary.slice().sort(function (a, b) { return Number(b.latest_share || 0) - Number(a.latest_share || 0); })[0];
    var largestIncrease = appendix.summary.slice().sort(function (a, b) { return Number(b.change_since_qt2_bn || -Infinity) - Number(a.change_since_qt2_bn || -Infinity); })[0];
    if (latest && largestIncrease) {
      insightTarget.innerHTML =
        '<strong>Sectoral readout:</strong> In <strong>' + appendix.metadata.latest_quarter + '</strong>, the largest published holder in this appendix is <strong>' +
        latest.sector + '</strong> at <strong>' + fmtPct(latest.latest_share) + '</strong> of private holdings. Since <strong>' +
        appendix.metadata.qt2_start_quarter + '</strong>, the biggest level increase is <strong>' + largestIncrease.sector +
        '</strong> at <strong>' + fmtSignedBn(largestIncrease.change_since_qt2_bn) + '</strong>, which is the cleanest sector-level companion to the dealer and cash-mechanics views.';
    }
  }

  function buildAuctionMixAppendix(appendix) {
    var mixTarget = el('auctionMixChart');
    var tenorTarget = el('auctionTenorChart');
    var summaryTarget = el('auctionSummaryTable');
    var tenorTableTarget = el('auctionTenorTable');
    var insightTarget = el('auctionInsight');
    if (!mixTarget || !tenorTarget || !summaryTarget || !tenorTableTarget || !insightTarget || !appendix || !appendix.series || !appendix.series.length) return;

    var c = cwColors();
    var series = appendix.series.filter(function (row) { return row.quarter >= '2009Q1'; });
    var qt2 = series.filter(function (row) { return row.quarter >= '2022Q2'; });
    var shareLabels = series.map(function (row) { return row.quarter; });
    var tenorLabels = qt2.map(function (row) { return row.quarter; });
    var tenorKeys = [
      ['coupon_2y_bn_share', '2y', c.blue],
      ['coupon_3y_bn_share', '3y', c.teal],
      ['coupon_5y_bn_share', '5y', c.green],
      ['coupon_7y_bn_share', '7y', c.amber],
      ['coupon_10y_bn_share', '10y', c.red],
      ['coupon_20y_bn_share', '20y', c.purple],
      ['coupon_30y_bn_share', '30y', '#64748b']
    ];

    instances.push(new Chart(mixTarget, {
      type: 'line',
      data: {
        labels: shareLabels,
        datasets: [
          { label: 'Bills', data: series.map(function (row) { return Number(row.bill_share) * 100; }), borderColor: c.blue, backgroundColor: c.blue + '18', borderWidth: 2, pointRadius: 0, fill: false, tension: 0 },
          { label: 'Coupons', data: series.map(function (row) { return Number(row.coupon_share) * 100; }), borderColor: c.teal, backgroundColor: c.teal + '18', borderWidth: 2, pointRadius: 0, fill: false, tension: 0 },
          { label: 'FRNs', data: series.map(function (row) { return Number(row.frn_share) * 100; }), borderColor: c.amber, backgroundColor: c.amber + '18', borderWidth: 2, pointRadius: 0, fill: false, tension: 0 }
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: true,
        aspectRatio: 1.8,
        animation: false,
        plugins: {
          legend: { position: 'bottom', labels: { color: c.textMuted, usePointStyle: true, padding: 12 } },
          title: { display: true, text: 'Realized Issuance Shares', color: c.text, font: { size: 14, weight: '600' }, padding: { bottom: 12 } },
          tooltip: { callbacks: { label: function (ctx) { return ctx.dataset.label + ': ' + fmt(ctx.parsed.y, 1) + '%'; } } }
        },
        scales: {
          x: { grid: { display: false }, ticks: { color: c.textMuted, maxTicksLimit: 8 } },
          y: { title: { display: true, text: '% of quarterly auction amount', color: c.textMuted }, grid: { color: c.grid }, ticks: { color: c.textMuted, callback: function (v) { return v + '%'; } }, min: 0, max: 100 }
        }
      }
    }));

    instances.push(new Chart(tenorTarget, {
      type: 'bar',
      data: {
        labels: tenorLabels,
        datasets: tenorKeys.map(function (spec) {
          return {
            label: spec[1],
            data: qt2.map(function (row) { return Number(row[spec[0]] || 0) * 100; }),
            backgroundColor: spec[2],
            borderWidth: 0
          };
        })
      },
      options: {
        responsive: true,
        maintainAspectRatio: true,
        aspectRatio: 1.8,
        animation: false,
        plugins: {
          legend: { position: 'bottom', labels: { color: c.textMuted, usePointStyle: true, padding: 12 } },
          title: { display: true, text: 'Fixed-Rate Coupon Tenor Shares Since QT2 Start', color: c.text, font: { size: 14, weight: '600' }, padding: { bottom: 12 } },
          tooltip: { callbacks: { label: function (ctx) { return ctx.dataset.label + ': ' + fmt(ctx.parsed.y, 1) + '%'; } } }
        },
        scales: {
          x: { stacked: true, grid: { display: false }, ticks: { color: c.textMuted, maxRotation: 45, font: { size: 10 } } },
          y: { stacked: true, title: { display: true, text: '% of fixed-rate coupon issuance', color: c.textMuted }, grid: { color: c.grid }, ticks: { color: c.textMuted, callback: function (v) { return v + '%'; } }, min: 0, max: 100 }
        }
      }
    }));

    var mixSummaryRows = appendix.summary.map(function (row) {
      return {
        metric: row.metric,
        latest_level: row.metric === 'Coupon WAM'
          ? (row.latest_amount_bn == null || isNaN(row.latest_amount_bn) ? '\u2014' : fmt(row.latest_amount_bn, 2) + ' years')
          : fmtBn(row.latest_amount_bn),
        latest_share: row.metric === 'Coupon WAM' ? '\u2014' : fmtPct(row.latest_share),
        change_since_qt2: row.metric === 'Coupon WAM'
          ? (row.change_since_qt2_share_pp == null || isNaN(row.change_since_qt2_share_pp) ? '\u2014' : fmt(row.change_since_qt2_share_pp, 2) + ' years')
          : fmtSignedPp(row.change_since_qt2_share_pp)
      };
    });

    summaryTarget.innerHTML =
      '<h3>Latest Quarter Mix Summary</h3>' +
      '<p style="margin:0 0 12px;color:var(--c-text-muted);font-size:0.9rem">Realized quarterly issuance shares grouped by settlement quarter.</p>' +
      htmlTable(
        mixSummaryRows,
        ['metric', 'latest_level', 'latest_share', 'change_since_qt2'],
        null,
        {
          metric: 'Metric',
          latest_level: 'Latest Level',
          latest_share: 'Latest Share',
          change_since_qt2: 'Change Since QT2 Start'
        }
      );

    tenorTableTarget.innerHTML =
      '<h3>Latest Quarter Coupon Tenor Split</h3>' +
      htmlTable(
        appendix.tenor_summary,
        ['tenor', 'latest_amount_bn', 'latest_share'],
        {
          latest_amount_bn: fmtBn,
          latest_share: fmtPct
        },
        {
          tenor: 'Tenor',
          latest_amount_bn: 'Latest Amount',
          latest_share: 'Share Of Coupons'
        }
      );

    var latest = series[series.length - 1];
    var qt2Start = qt2.length ? qt2[0] : series[0];
    var largestTenor = appendix.tenor_summary.slice().sort(function (a, b) { return Number(b.latest_share || 0) - Number(a.latest_share || 0); })[0];
    if (latest && qt2Start && largestTenor) {
      insightTarget.innerHTML =
        '<strong>Auction-mix readout:</strong> In <strong>' + appendix.metadata.latest_quarter + '</strong>, bills accounted for <strong>' + fmtPct(latest.bill_share) +
        '</strong> of realized issuance versus <strong>' + fmtPct(qt2Start.bill_share) + '</strong> in <strong>' + appendix.metadata.qt2_start_quarter +
        '</strong>. The largest fixed-rate coupon bucket in the latest quarter is <strong>' + largestTenor.tenor + '</strong> at <strong>' + fmtPct(largestTenor.latest_share) +
        '</strong> of coupon issuance. This gives the site a realized mix panel alongside the refunding-guidance and duration-shock measures.';
    }
  }

  /* ================================================================
     Table builders
     ================================================================ */

  function htmlTable(rows, columns, formatters, colLabels) {
    if (!rows || !rows.length) return '<p class="loading">No data</p>';
    var cols = columns || Object.keys(rows[0]);
    var head = '<tr>' + cols.map(function (c) { return '<th>' + ((colLabels && colLabels[c]) || c.replace(/_/g, ' ')) + '</th>'; }).join('') + '</tr>';
    var body = rows.map(function (row) {
      return '<tr>' + cols.map(function (c) {
        var v = row[c];
        if (formatters && formatters[c]) v = formatters[c](v);
        else if (typeof v === 'number') v = fmt(v, v === Math.round(v) ? 0 : 2);
        else if (v == null) v = '\u2014';
        return '<td>' + v + '</td>';
      }).join('') + '</tr>';
    }).join('');
    return '<div class="table-wrap"><table class="data-table"><thead>' + head + '</thead><tbody>' + body + '</tbody></table></div>';
  }

  function buildRegimeTable(data) {
    var cols = ['fed_regime', 'n_weeks', 'soma_treasuries_bn_mean', 'reserves_bn_mean', 'on_rrp_bn_mean', 'dealer_inventory_bn_mean', 'repo_spread_bp_mean'];
    var colLabels = {
      fed_regime: 'Regime', n_weeks: 'Weeks',
      soma_treasuries_bn_mean: 'SOMA', reserves_bn_mean: 'Reserves',
      on_rrp_bn_mean: 'ON RRP', dealer_inventory_bn_mean: 'Dealers',
      repo_spread_bp_mean: 'Spread'
    };
    var fmters = {
      fed_regime: function (v) { return v ? v.replace(/_/g, ' ') : '\u2014'; },
      soma_treasuries_bn_mean: fmtBn,
      reserves_bn_mean: fmtBn,
      on_rrp_bn_mean: fmtBn,
      dealer_inventory_bn_mean: fmtBn,
      repo_spread_bp_mean: function (v) { return fmt(v, 1) + ' bp'; }
    };
    var target = el('regimeTable');
    if (target) target.innerHTML = '<h3>Regime Averages</h3>' + htmlTable(data, cols, fmters, colLabels);
  }

  function buildEpisodeTable(data) {
    var cols = ['episode_name', 'n_weeks', 'soma_change_bn', 'dealer_inventory_bn', 'repo_spread_bp', 'on_rrp_bn'];
    var colLabels = {
      episode_name: 'Episode', n_weeks: 'Weeks',
      soma_change_bn: 'SOMA \u0394', dealer_inventory_bn: 'Dealers',
      repo_spread_bp: 'Spread', on_rrp_bn: 'ON RRP'
    };
    var fmters = {
      soma_change_bn: fmtBn,
      dealer_inventory_bn: fmtBn,
      on_rrp_bn: fmtBn,
      repo_spread_bp: function (v) { return fmt(v, 1) + ' bp'; }
    };
    var target = el('episodeTable');
    if (target) target.innerHTML = '<h3>Episode Comparison</h3>' + htmlTable(data, cols, fmters, colLabels);
  }

  function buildQtCompareTable(data) {
    var target = el('qtCompareTable');
    if (!target || !data || !data.length) return;
    var cols = [
      'regime', 'comparison_weeks', 'soma_change_bn', 'on_rrp_change_bn',
      'dealer_inventory_change_bn', 'repo_spread_bp_mean', 'qt_runoff_dv01_cum'
    ];
    var colLabels = {
      regime: 'Window',
      comparison_weeks: 'Weeks',
      soma_change_bn: 'SOMA \u0394',
      on_rrp_change_bn: 'ON RRP \u0394',
      dealer_inventory_change_bn: 'Dealers \u0394',
      repo_spread_bp_mean: 'Avg Spread',
      qt_runoff_dv01_cum: 'Cum Runoff'
    };
    var fmters = {
      soma_change_bn: fmtBn,
      on_rrp_change_bn: fmtBn,
      dealer_inventory_change_bn: fmtBn,
      repo_spread_bp_mean: function (v) { return fmt(v, 1) + ' bp'; },
      qt_runoff_dv01_cum: function (v) { return fmt(v, 1); }
    };
    target.innerHTML =
      '<h3>QT1 vs QT2 (First 52 Weeks)</h3>' +
      '<p style="margin:0 0 12px;color:var(--c-text-muted);font-size:0.9rem">Each row uses the first 52 weekly observations of that runoff episode so the balance-sheet adjustments are compared on a normalized clock.</p>' +
      htmlTable(data, cols, fmters, colLabels);
  }

  function buildCorrTable(data) {
    var target = el('corrTable');
    if (!target || !data || !data.length) return;
    var cols = Object.keys(data[0]);
    var labels = cols.map(function (c) { return c.replace(/_bn$/, '').replace(/_bp$/, '').replace(/_/g, ' '); });
    var head = '<tr><th></th>' + labels.map(function (l) { return '<th>' + l + '</th>'; }).join('') + '</tr>';
    var body = data.map(function (row, i) {
      return '<tr><td style="font-weight:600;font-family:inherit;font-size:0.78rem">' + labels[i] + '</td>' +
        cols.map(function (c) {
          var v = Number(row[c]);
          var abs = Math.abs(v);
          var bg = abs > 0.8 ? 'rgba(220,38,38,0.15)' : abs > 0.5 ? 'rgba(217,119,6,0.12)' : '';
          return '<td style="' + (bg ? 'background:' + bg : '') + '">' + v.toFixed(3) + '</td>';
        }).join('') + '</tr>';
    }).join('');
    target.innerHTML = '<h3>QT2 Correlations</h3><div class="table-wrap"><table class="data-table"><thead>' + head + '</thead><tbody>' + body + '</tbody></table></div>';
  }

  function buildReactionTable(data) {
    var target = el('reactionTable');
    if (!target || !data || !data.length) return;
    var cols = ['term', 'coef', 'std_err', 'p_value'];
    var nObs = data[0].n_obs;
    var rSquared = data[0].r_squared;
    var fmters = {
      coef: function (v) { return Number(v).toFixed(4); },
      std_err: function (v) { return Number(v).toFixed(4); },
      p_value: function (v) {
        var p = Number(v);
        var star = p < 0.01 ? ' ***' : p < 0.05 ? ' **' : p < 0.1 ? ' *' : '';
        return p.toFixed(4) + star;
      }
    };
    target.innerHTML =
      '<h3>Treasury Reaction Function (OLS, HC3)</h3>' +
      '<p style="margin:0 0 12px;color:var(--c-text-muted);font-size:0.9rem">Quarterly sample: <strong>' + fmt(nObs, 0) + '</strong> observations. Model fit: <strong>R\u00b2 ' + fmt(rSquared, 2) + '</strong>.</p>' +
      htmlTable(data, cols, fmters);
  }

  function buildLpTable(data, targetId, title) {
    var target = el(targetId);
    if (!target || !data || !data.length) return;
    var rows = data.map(function (row) {
      var out = Object.assign({}, row);
      out.ci_lo = row.ci_lo != null ? row.ci_lo : row.ci_lower_95;
      out.ci_hi = row.ci_hi != null ? row.ci_hi : row.ci_upper_95;
      return out;
    });
    var cols = ['horizon', 'term', 'coef', 'ci_lo', 'ci_hi'];
    var nObs = rows[0].n_obs;
    var rSquared = rows[0].r_squared;
    var fmters = {
      coef: function (v) { return Number(v).toFixed(4); },
      ci_lo: function (v) { return Number(v).toFixed(4); },
      ci_hi: function (v) { return Number(v).toFixed(4); }
    };
    target.innerHTML =
      '<h3>' + title + '</h3>' +
      '<p style="margin:0 0 12px;color:var(--c-text-muted);font-size:0.9rem">Weekly sample: <strong>' + fmt(nObs, 0) + '</strong> observations at horizon 0. Baseline fit at horizon 0: <strong>R\u00b2 ' + fmt(rSquared, 2) + '</strong>.</p>' +
      htmlTable(rows, cols, fmters);
  }

  function buildMeasurementNotes(summary) {
    var notesTarget = el('measurementNotes');
    var reproTarget = el('reproNotes');
    if (!summary) return;

    if (notesTarget) {
      var notes = summary.measurement_notes || [];
      var notesHtml = notes.map(function (note) {
        return '<div class="note-item"><strong>' + note.title + '</strong><p>' + note.detail + '</p></div>';
      }).join('');
      notesTarget.innerHTML = '<h3 class="note-card-title">Construction And Caveats</h3><div class="note-list">' + notesHtml + '</div>';
    }

    if (reproTarget) {
      var hashes = (summary.artifact_hashes || []).slice(0, 6);
      var runoffCounts = summary.runoff_source_counts || {};
      var hashRows = hashes.map(function (row) {
        return '<tr><td>' + row.file + '</td><td>' + row.sha256.slice(0, 12) + '</td></tr>';
      }).join('');
      var runoffRows = Object.keys(runoffCounts).map(function (key) {
        return '<tr><td>' + key.replace(/_/g, ' ') + '</td><td>' + runoffCounts[key] + '</td></tr>';
      }).join('');
      var generated = summary.generated_at_utc || '\u2014';
      reproTarget.innerHTML =
        '<h3 class="note-card-title">Build Metadata</h3>' +
        '<div class="meta-block"><strong>Generated</strong><p>' + generated + '</p></div>' +
        '<div class="meta-block"><strong>Public window</strong><p>' + (summary.site_window_start || '\u2014') + ' onward, ' + (summary.weekly_frequency || '\u2014') + ' sampling</p></div>' +
        '<div class="meta-block"><strong>Coverage</strong><p>Quarterly panel: ' + (summary.quarterly_window_start || '\u2014') + '. Dealer series: ' + (summary.dealer_window_start || '\u2014') + '. Repo spread: ' + (summary.repo_window_start || '\u2014') + '.</p></div>' +
        '<div class="meta-block"><strong>Runoff coverage</strong><table class="artifact-table"><tbody>' + runoffRows + '</tbody></table></div>' +
        '<div class="meta-block"><strong>Artifact hashes</strong><table class="artifact-table"><tbody>' + hashRows + '</tbody></table></div>';
    }
  }

  function buildManualAuditAppendix(audit) {
    var target = el('manualAuditAppendix');
    if (!target || !audit) return;

    var summary = audit.summary || {};
    var rows = audit.rows || [];
    var chips = [
      { value: summary.manual_quarter_rows, label: 'Manual quarter rows' },
      { value: summary.verified_rows, label: 'Verified rows' },
      { value: summary.debt_limit_rows, label: 'Debt-limit rows' },
      { value: summary.cash_balance_statement_sourced_rows, label: 'Cash-balance rows sourced from statement text' }
    ];
    var chipsHtml = chips.map(function (chip) {
      return '<div class="provenance-chip"><strong>' + fmt(chip.value, 0) + '</strong><span>' + chip.label + '</span></div>';
    }).join('');

    var notes = summary.workflow_notes || [];
    var notesHtml = notes.length
      ? '<ul class="provenance-list">' + notes.map(function (note) { return '<li>' + note + '</li>'; }).join('') + '</ul>'
      : '';

    var tableRows = rows.map(function (row) {
      return {
        quarter: row.quarter,
        refunding_date: row.refunding_date,
        verification_status: row.verification_status,
        debt_limit_flag: Number(row.debt_limit_flag) === 1 ? 'Yes' : 'No',
        statement_url: row.statement_url ? '<a href="' + row.statement_url + '" target="_blank" rel="noopener noreferrer">Source</a>' : '\u2014',
        reviewer_notes: row.reviewer_notes || '\u2014'
      };
    });

    var colLabels = {
      quarter: 'Quarter',
      refunding_date: 'Date',
      verification_status: 'Status',
      debt_limit_flag: 'Debt Limit',
      statement_url: 'Statement',
      reviewer_notes: 'Review Notes'
    };

    target.innerHTML =
      '<h3 class="note-card-title">Manual Input Provenance</h3>' +
      '<p style="margin:0 0 12px;color:var(--c-text-muted);font-size:0.9rem">Quarterly refunding inputs still include hand-reviewed source verification. This appendix shows the public provenance fields carried from the manual overrides file.</p>' +
      '<div class="provenance-summary">' + chipsHtml + '</div>' +
      notesHtml +
      '<div class="provenance-table">' + htmlTable(tableRows, ['quarter', 'refunding_date', 'verification_status', 'debt_limit_flag', 'statement_url', 'reviewer_notes'], null, colLabels) + '</div>';
  }

  function corrLookup(matrix, rowKey, colKey) {
    if (!matrix || !matrix.length) return null;
    var cols = Object.keys(matrix[0]);
    var rowIndex = cols.indexOf(rowKey);
    if (rowIndex === -1) return null;
    return matrix[rowIndex][colKey];
  }

  function buildInsights(weekly, qDesc, corr) {
    var dealerTarget = el('dealerInsight');
    var repoTarget = el('repoInsight');
    var durationTarget = el('durationInsight');
    var bufferTarget = el('bufferInsight');

    var somaDealerCorr = corrLookup(corr, 'soma_treasuries_bn', 'dealer_inventory_bn');
    if (dealerTarget) {
      dealerTarget.innerHTML = '<strong>During QT2</strong>, the correlation between SOMA holdings and dealer inventories is <strong>r = ' + fmtCorr(somaDealerCorr) + '</strong>. As SOMA holdings fall, dealer inventories tend to rise.';
    }

    var repoDealerCorr = corrLookup(corr, 'repo_spread_bp', 'dealer_inventory_bn');
    var repoLiquidityCorr = corrLookup(corr, 'repo_spread_bp', 'system_liquidity_bn');
    if (repoTarget) {
      repoTarget.innerHTML = '<strong>During QT2</strong>, repo spreads correlate <strong>r = ' + fmtCorr(repoDealerCorr) + '</strong> with dealer inventories and <strong>r = ' + fmtCorr(repoLiquidityCorr) + '</strong> with system liquidity. Tighter balance-sheet capacity lines up with wider funding spreads.';
    }

    if (durationTarget && qDesc && qDesc.length) {
      var quarters = qDesc.slice().sort(function (a, b) { return String(a.quarter).localeCompare(String(b.quarter)); });
      var negative = quarters.filter(function (row) { return row.net_private_duration_dv01 != null && Number(row.net_private_duration_dv01) < 0; });
      if (negative.length) {
        var firstNeg = negative[0];
        durationTarget.innerHTML = 'Net private duration supply first turns negative in <strong>' + firstNeg.quarter + '</strong> at <strong>' + fmt(Number(firstNeg.net_private_duration_dv01), 0) + ' DV01</strong>. In those quarters, buybacks and issuance choices reduced the private sector&rsquo;s net duration burden.';
      } else {
        var latest = quarters[quarters.length - 1];
        durationTarget.innerHTML = 'Net private duration supply remains positive through <strong>' + latest.quarter + '</strong>. The published sample still shows a positive private-sector duration burden overall.';
      }
    }

    var tgaReservesCorr = corrLookup(corr, 'tga_bn', 'reserves_bn');
    if (bufferTarget) {
      bufferTarget.innerHTML = '<strong>TGA-reserves relationship:</strong> During QT2, the simple correlation between TGA and reserves is <strong>r = ' + fmtCorr(tgaReservesCorr) + '</strong>. TGA swings matter for reserve conditions, but the pass-through is not one-for-one in this sample.';
    }
  }

  /* ================================================================
     Stat chips
     ================================================================ */
  function computeStats(weekly, qDesc) {
    // SOMA decline during QT2 (June 2022 onward)
    var qt2w = weekly.filter(function (r) { return r.week >= '2022-06-01' && r.soma_treasuries_bn != null; });
    if (qt2w.length > 1) {
      var somaStart = qt2w[0].soma_treasuries_bn;
      var somaEnd = qt2w[qt2w.length - 1].soma_treasuries_bn;
      var s = el('stat-soma');
      if (s) s.textContent = fmtBn(somaEnd - somaStart);
    }

    // Dealer inventory latest
    var dealers = weekly.filter(function (r) { return r.dealer_inventory_bn != null; });
    if (dealers.length) {
      var d = el('stat-dealer');
      if (d) d.textContent = fmtBn(dealers[dealers.length - 1].dealer_inventory_bn);
    }

    // ON RRP drained
    var onrrp = weekly.filter(function (r) { return r.on_rrp_bn != null; });
    if (onrrp.length) {
      var peak = Math.max.apply(null, onrrp.map(function (r) { return r.on_rrp_bn; }));
      var latest = onrrp[onrrp.length - 1].on_rrp_bn;
      var o = el('stat-onrrp');
      if (o) o.textContent = fmtBn(peak - latest);
    }

    // Repo spread latest
    var spreads = weekly.filter(function (r) { return r.repo_spread_bp != null; });
    if (spreads.length) {
      var sp = el('stat-spread');
      if (sp) sp.textContent = fmt(spreads[spreads.length - 1].repo_spread_bp, 0) + ' bp';
    }
  }

  /* ================================================================
     Orchestration
     ================================================================ */
  var _data = null;

  function destroyAll() {
    instances.forEach(function (c) { c.destroy(); });
    instances.length = 0;
  }

  function buildAll(d) {
    destroyAll();
    applyDefaults();
    computeStats(d.weekly, d.qDesc);

    buildSoma(d.weekly);
    buildOnRrp(d.weekly);
    buildDealer(d.weekly);
    buildRepo(d.weekly);
    buildNetDuration(d.qDesc);
    buildOnRrpShare(d.qDesc);
    buildTga(d.weekly);
    buildCashMechanicsAppendix(d.dailyAppendix);
    buildCashValidationAppendix(d.dailyValidation);
    buildSectoralAbsorbersAppendix(d.sectoralAppendix);
    buildAuctionMixAppendix(d.auctionMixAppendix);
    buildChannelDashboard(d.weekly);

    buildRegimeTable(d.regime);
    buildQtCompareTable(d.qtCompare);
    buildEpisodeTable(d.episodes);
    buildCorrTable(d.corr);
    buildReactionTable(d.reaction);
    buildLpTable(d.lpDealer, 'lpDealerTable', 'Local Projection: Dealer Inventory');
    buildLpTable(d.lpRepo, 'lpRepoTable', 'Local Projection: Repo Spread');
    buildInsights(d.weekly, d.qDesc, d.corr);
    buildMeasurementNotes(d.summary);
    buildManualAuditAppendix(d.manualAudit);
  }

  window.cwRebuildCharts = function () {
    if (_data) buildAll(_data);
  };

  /* ================================================================
     Nav & collapsible interactivity
     ================================================================ */
  function setupNav() {
    var btn = el('nav-toggle');
    var links = document.querySelector('.nav-links');
    if (btn && links) {
      btn.addEventListener('click', function () { links.classList.toggle('open'); });
      links.querySelectorAll('a').forEach(function (a) {
        a.addEventListener('click', function () { links.classList.remove('open'); });
      });
      document.addEventListener('keydown', function (e) {
        if (e.key === 'Escape') links.classList.remove('open');
      });
    }

    // Collapsibles
    document.querySelectorAll('.collapsible-header').forEach(function (hdr) {
      hdr.addEventListener('click', function () {
        hdr.parentElement.classList.toggle('open');
      });
    });

    // Scroll spy
    var sections = document.querySelectorAll('section[id]');
    var navAs = document.querySelectorAll('.nav-links a');
    if (sections.length && navAs.length) {
      var obs = new IntersectionObserver(function (entries) {
        entries.forEach(function (ent) {
          if (ent.isIntersecting) {
            navAs.forEach(function (a) {
              a.classList.toggle('active', a.getAttribute('href') === '#' + ent.target.id);
            });
          }
        });
      }, { rootMargin: '-80px 0px -60% 0px' });
      sections.forEach(function (s) { obs.observe(s); });
    }
  }

  /* ================================================================
     Init
     ================================================================ */
  document.addEventListener('DOMContentLoaded', function () {
    setupNav();
    loadAll().then(function (d) {
      _data = d;
      // Remove loading placeholders
      document.querySelectorAll('.loading').forEach(function (el) { el.remove(); });
      buildAll(d);
    }).catch(function (err) {
      console.error('Data load failed:', err);
      document.querySelectorAll('.loading').forEach(function (el) {
        el.textContent = 'Failed to load data. Serve this directory with a local HTTP server.';
      });
    });
  });
})();
