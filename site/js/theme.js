/* theme.js — loaded synchronously in <head> to prevent FOUC */
(function () {
  var KEY = 'coordwatch-theme';

  function initial() {
    var s = localStorage.getItem(KEY);
    if (s === 'dark' || s === 'light') return s;
    return 'light';
  }

  var theme = initial();
  document.documentElement.setAttribute('data-theme', theme);

  /* ---- public API ---- */

  window.cwToggleTheme = function () {
    var cur = document.documentElement.getAttribute('data-theme');
    var next = cur === 'dark' ? 'light' : 'dark';
    document.documentElement.setAttribute('data-theme', next);
    localStorage.setItem(KEY, next);
    btnText(next);
    if (typeof cwRebuildCharts === 'function') cwRebuildCharts();
  };

  window.cwGetTheme = function () {
    return document.documentElement.getAttribute('data-theme') || 'light';
  };

  /* Theme-aware colour palette consumed by charts.js */
  window.cwColors = function () {
    var dk = cwGetTheme() === 'dark';
    return {
      teal:      dk ? '#2dd4bf' : '#0d9488',
      blue:      dk ? '#60a5fa' : '#3b82f6',
      amber:     dk ? '#fbbf24' : '#d97706',
      red:       dk ? '#f87171' : '#dc2626',
      green:     dk ? '#4ade80' : '#16a34a',
      purple:    dk ? '#c084fc' : '#9333ea',
      slate:     dk ? '#94a3b8' : '#64748b',
      text:      dk ? '#e2e8f0' : '#1e293b',
      textMuted: dk ? '#94a3b8' : '#64748b',
      grid:      dk ? 'rgba(148,163,184,0.12)' : 'rgba(0,0,0,0.06)',
      tooltipBg: '#0f172a',
      tooltipTitle: '#ffffff',
      tooltipBody: '#e0e0e0',
      regimeQE:    dk ? 'rgba(45,212,191,0.10)' : 'rgba(13,148,136,0.07)',
      regimeQT:    dk ? 'rgba(248,113,113,0.10)' : 'rgba(220,38,38,0.07)',
      regimeTaper: dk ? 'rgba(251,191,36,0.10)' : 'rgba(217,119,6,0.07)',
      palette: dk
        ? ['#2dd4bf','#60a5fa','#fbbf24','#f87171','#c084fc','#4ade80','#fb923c','#e879f9']
        : ['#0d9488','#3b82f6','#d97706','#dc2626','#9333ea','#16a34a','#ea580c','#c026d3']
    };
  };

  /* ---- internals ---- */

  function btnText(t) {
    var b = document.getElementById('theme-toggle');
    if (b) b.textContent = t === 'dark' ? '\u2600' : '\u263E';
  }

  var mq = window.matchMedia('(prefers-color-scheme: dark)');
  mq.addEventListener('change', function (e) {
    if (!localStorage.getItem(KEY)) {
      var t = e.matches ? 'dark' : 'light';
      document.documentElement.setAttribute('data-theme', t);
      btnText(t);
      if (typeof cwRebuildCharts === 'function') cwRebuildCharts();
    }
  });

  function wireToggle() {
    btnText(theme);
    var b = document.getElementById('theme-toggle');
    if (b) b.addEventListener('click', cwToggleTheme);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', wireToggle);
  } else {
    wireToggle();
  }
})();
