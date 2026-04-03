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
      fetchJ('summary')
    ]).then(function (arr) {
      return {
        weekly: arr[0], qDesc: arr[1], regime: arr[2],
        qtCompare: arr[3], episodes: arr[4], corr: arr[5], reaction: arr[6],
        lpDealer: arr[7], lpRepo: arr[8], summary: arr[9]
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
    if (!target) return;
    var cols = ['term', 'coef', 'std_err', 'p_value'];
    var fmters = {
      coef: function (v) { return Number(v).toFixed(4); },
      std_err: function (v) { return Number(v).toFixed(4); },
      p_value: function (v) {
        var p = Number(v);
        var star = p < 0.01 ? ' ***' : p < 0.05 ? ' **' : p < 0.1 ? ' *' : '';
        return p.toFixed(4) + star;
      }
    };
    target.innerHTML = '<h3>Treasury Reaction Function (OLS, HC3)</h3>' + htmlTable(data, cols, fmters);
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
    var fmters = {
      coef: function (v) { return Number(v).toFixed(4); },
      ci_lo: function (v) { return Number(v).toFixed(4); },
      ci_hi: function (v) { return Number(v).toFixed(4); }
    };
    target.innerHTML = '<h3>' + title + '</h3>' + htmlTable(rows, cols, fmters);
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
        '<div class="meta-block"><strong>Runoff coverage</strong><table class="artifact-table"><tbody>' + runoffRows + '</tbody></table></div>' +
        '<div class="meta-block"><strong>Artifact hashes</strong><table class="artifact-table"><tbody>' + hashRows + '</tbody></table></div>';
    }
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

    buildRegimeTable(d.regime);
    buildQtCompareTable(d.qtCompare);
    buildEpisodeTable(d.episodes);
    buildCorrTable(d.corr);
    buildReactionTable(d.reaction);
    buildLpTable(d.lpDealer, 'lpDealerTable', 'Local Projection: Dealer Inventory');
    buildLpTable(d.lpRepo, 'lpRepoTable', 'Local Projection: Repo Spread');
    buildInsights(d.weekly, d.qDesc, d.corr);
    buildMeasurementNotes(d.summary);
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
