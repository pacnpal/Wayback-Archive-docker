// Compile simple schedule-builder fields → cron, and translate cron → human.
// Exposes window.scheduleBuilder.{compile, humanise} for template hooks.
(function() {
  const DOW_SHORT = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
  const DOW_LONG  = ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'];

  function compile(fields) {
    const { mode, minute, time, dows, dom } = fields;
    const [hh, mm] = (time || '03:00').split(':').map(n => parseInt(n, 10) || 0);
    const m = Math.max(1, Math.min(59, parseInt(minute, 10) || 1));
    const d = Math.max(1, Math.min(31, parseInt(dom, 10) || 1));
    switch (mode) {
      case 'every-n': return `*/${m} * * * *`;
      case 'hourly':  return `${Math.min(59, m)} * * * *`;
      case 'daily':   return `${mm} ${hh} * * *`;
      case 'weekly': {
        const days = (dows && dows.length) ? [...dows].sort((a,b)=>a-b).join(',') : '1';
        return `${mm} ${hh} * * ${days}`;
      }
      case 'monthly': return `${mm} ${hh} ${d} * *`;
    }
    return '';
  }

  function pad(n) { return String(n).padStart(2, '0'); }

  function humanise(cron) {
    if (!cron) return '';
    const parts = cron.trim().split(/\s+/);
    if (parts.length !== 5) return '(custom cron)';
    const [mi, hr, dom, mo, dow] = parts;

    // every N minutes
    let m;
    if ((m = mi.match(/^\*\/(\d+)$/)) && hr === '*' && dom === '*' && mo === '*' && dow === '*') {
      return `every ${m[1]} minute${m[1] === '1' ? '' : 's'}`;
    }
    // Hourly at minute N
    if (/^\d+$/.test(mi) && hr === '*' && dom === '*' && mo === '*' && dow === '*') {
      return `every hour at :${pad(parseInt(mi, 10))}`;
    }
    // Daily
    if (/^\d+$/.test(mi) && /^\d+$/.test(hr) && dom === '*' && mo === '*' && dow === '*') {
      return `every day at ${pad(hr)}:${pad(mi)}`;
    }
    // Weekly
    if (/^\d+$/.test(mi) && /^\d+$/.test(hr) && dom === '*' && mo === '*' && /^[\d,]+$/.test(dow)) {
      const days = dow.split(',').map(d => DOW_LONG[parseInt(d, 10) % 7]);
      return `every ${days.join(', ')} at ${pad(hr)}:${pad(mi)}`;
    }
    // Monthly
    if (/^\d+$/.test(mi) && /^\d+$/.test(hr) && /^\d+$/.test(dom) && mo === '*' && dow === '*') {
      return `day ${dom} of every month at ${pad(hr)}:${pad(mi)}`;
    }
    return '(custom cron)';
  }

  function wireForm(root) {
    const form = root.querySelector('form.schedule-builder');
    if (!form || form.dataset.wired) return;
    form.dataset.wired = '1';

    const byName = n => form.querySelector(`[name="${n}"]`);
    const findAll = n => form.querySelectorAll(`[name="${n}"]`);
    const preview = form.querySelector('.sb-preview');
    const hiddenCron = form.querySelector('input[name="cron_expr"]');
    const advanced = form.querySelector('.sb-advanced input[name="cron_expr_raw"]');

    function readSimple() {
      const mode = form.querySelector('[name="mode"]:checked')?.value || 'daily';
      return {
        mode,
        minute: byName('minute')?.value || '15',
        time:   byName('time')?.value || '03:00',
        dows:   [...form.querySelectorAll('[name="dow"]:checked')].map(el => parseInt(el.value, 10)),
        dom:    byName('dom')?.value || '1',
      };
    }

    function toggleMode(mode) {
      form.querySelectorAll('[data-mode]').forEach(el => {
        el.hidden = !el.dataset.mode.split(/\s+/).includes(mode);
      });
    }

    function refresh() {
      if (form.dataset.tab === 'advanced') {
        const raw = advanced?.value.trim() || '';
        hiddenCron.value = raw;
        preview.textContent = raw ? `→ ${humanise(raw)}` : '';
        return;
      }
      const s = readSimple();
      toggleMode(s.mode);
      const cron = compile(s);
      hiddenCron.value = cron;
      preview.textContent = cron ? `→ cron: ${cron}  (${humanise(cron)})` : '';
    }

    form.addEventListener('input', refresh);
    form.addEventListener('change', refresh);

    // Tab toggle
    const simpleLink = form.querySelector('[data-tab="simple"]');
    const advLink = form.querySelector('[data-tab="advanced"]');
    function setTab(t) {
      form.dataset.tab = t;
      form.querySelector('.sb-simple').hidden = t !== 'simple';
      form.querySelector('.sb-advanced').hidden = t !== 'advanced';
      simpleLink?.classList.toggle('active', t === 'simple');
      advLink?.classList.toggle('active', t === 'advanced');
      refresh();
    }
    simpleLink?.addEventListener('click', e => { e.preventDefault(); setTab('simple'); });
    advLink?.addEventListener('click', e => { e.preventDefault(); setTab('advanced'); });

    setTab('simple');
  }

  window.scheduleBuilder = { compile, humanise, wireForm };
  document.addEventListener('DOMContentLoaded', () => wireForm(document));
})();
