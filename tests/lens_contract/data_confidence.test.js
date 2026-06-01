// AO1+AO2 verification — runs _renderDataConfidence against four port
// fixtures and confirms structure, wording, and top-level state logic.
const fs = require('fs');
const html = fs.readFileSync('/Users/tonytrajceski/Documents/Claude/Projects/Project Horizon/index.html','utf-8');

// Extract _renderDataConfidence function source.
const startMarker = '  function _renderDataConfidence(){';
const endMarker   = '\n  }\n'; // first top-level closing of the function
const si = html.indexOf(startMarker);
const ei = html.indexOf('\n  }', si) + 4; // include the closing brace
const fnSrc = html.slice(si, ei);

// Mock minimal DOM (document.getElementById returns objects we control)
function makeDom(){
  const elems = {
    'dc-body':  { innerHTML: '' },
    'dc-state': { textContent: '', className: '' }
  };
  return {
    getElementById: id => elems[id] || null,
    _elems: elems
  };
}

// Each fixture is tested with a fresh DOM.
function runFor(port){
  const summary = JSON.parse(fs.readFileSync(`/tmp/dc_${port}.json`,'utf-8'));
  const dom = makeDom();
  // Build a context that supplies _last + esc + the DOM stubs
  const ctxBody = `
    const document = arguments[0];
    const _last = arguments[1];
    function esc(s){return String(s==null?"":s).replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));}
    ${fnSrc}
    _renderDataConfidence();
    return { state: document._elems['dc-state'], body: document._elems['dc-body'] };
  `;
  const f = new Function(ctxBody);
  return f(dom, summary);
}

const RESULTS = [];
function check(label, ok, detail){ RESULTS.push({label, ok, detail}); }

const ports = ['BRISBANE','MELBOURNE','GEELONG','DARWIN'];
for(const port of ports){
  const { state, body } = runFor(port);
  console.log(`\n=== ${port} ===`);
  console.log(`  state.textContent: ${state.textContent}`);
  console.log(`  state.className:   ${state.className}`);
  // Extract row labels + state pills
  const rowPattern = /<div class="dc-row-label">([^<]+?)\s*<div class="dc-row-detail">([^<]+?)<\/div>\s*<\/div>\s*<span class="dc-row-state dc-state-([a-z]+)">([^<]+?)<\/span>/gs;
  let m, rows = [];
  while((m = rowPattern.exec(body.innerHTML)) !== null){
    rows.push({ label: m[1].trim(), detail: m[2], stateCls: m[3], stateLabel: m[4] });
  }
  rows.forEach(r => console.log(`  - ${r.label.padEnd(22)} | ${r.stateLabel.padEnd(20)} | ${r.detail}`));

  // Assertions per port:
  check(`${port}: 5 rows rendered`, rows.length === 5, `got ${rows.length}`);
  check(`${port}: Vessel movements row present`, rows.some(r => r.label === 'Vessel movements'), '');
  check(`${port}: Tides row present`, rows.some(r => r.label === 'Tides'), '');
  check(`${port}: Weather row present`, rows.some(r => r.label === 'Weather'), '');
  check(`${port}: Towage availability is Assumed`, rows.find(r => r.label === 'Towage availability')?.stateLabel === 'Assumed', '');
  check(`${port}: Terminal readiness is Assumed`, rows.find(r => r.label === 'Terminal readiness')?.stateLabel === 'Assumed', '');
  // Tides + Weather use "Live environmental" or "Assumed" label
  const tidesRow = rows.find(r => r.label === 'Tides');
  check(`${port}: Tides label is 'Live environmental' or 'Assumed'`,
    tidesRow && (tidesRow.stateLabel === 'Live environmental' || tidesRow.stateLabel === 'Assumed'), `got ${tidesRow?.stateLabel}`);
  const weatherRow = rows.find(r => r.label === 'Weather');
  check(`${port}: Weather label is 'Live environmental' or 'Assumed'`,
    weatherRow && (weatherRow.stateLabel === 'Live environmental' || weatherRow.stateLabel === 'Assumed'), `got ${weatherRow?.stateLabel}`);
  // Vessel movements uses "Live observed", "Assumed", or "Missing"
  const vesRow = rows.find(r => r.label === 'Vessel movements');
  check(`${port}: Vessel movements label is one of {Live observed, Assumed, Missing}`,
    vesRow && ['Live observed','Assumed','Missing'].includes(vesRow.stateLabel), `got ${vesRow?.stateLabel}`);
  // Top-level state matches the count of live feeds
  const liveCount = rows.filter(r => ['Tides','Weather','Vessel movements'].includes(r.label) && r.stateCls === 'live').length;
  const expectedTop = liveCount === 3 ? 'High' : liveCount > 0 ? 'Medium' : 'Low';
  check(`${port}: top-level state matches live-count derivation`,
    state.textContent === expectedTop, `expected ${expectedTop} got ${state.textContent} (liveCount=${liveCount})`);
  // No "Beta 11" or "Authority" or scoring leaks
  const banned = ['Beta 11','Authority','%','Trust','Score'];
  banned.forEach(s => check(`${port}: no '${s}' in body`, body.innerHTML.indexOf(s) < 0, ''));
}

console.log('\n=== ACCEPTANCE TOTALS ===');
const pass = RESULTS.filter(r => r.ok).length;
const fail = RESULTS.length - pass;
console.log(`${pass} pass / ${fail} fail / ${RESULTS.length} total`);
RESULTS.filter(r => !r.ok).forEach(r => console.log(`  FAIL [${r.label}] — ${r.detail}`));
process.exit(fail > 0 ? 1 : 0);
