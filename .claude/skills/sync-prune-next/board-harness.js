// Runs board.html's inline script against a stub DOM and prints what it drew,
// as JSON, so a page change can be checked without a browser or node. Usage:
//
//   osascript -l JavaScript board-harness.js PAGE MODE SHOW_ALL MILESTONE [DATA]
//
// PAGE is the page file. MODE is how the database answers:
//   none     the view has no database
//   loading  the database never answers
//   empty    the document does not exist
//   shape    the document fails `usable`
//   stopped  the subscription errors before any data
//   live     DATA is delivered
//   dropped  DATA is delivered, then the subscription errors
// SHOW_ALL and MILESTONE are the saved "Show all" and milestone choices, or "-"
// for none saved. DATA is a JSON file holding the board document.
//
// The output maps each element id to its markup, its text and whether it is
// hidden. `visible` keeps only the elements whose static ancestors are drawn
// too, with markup stripped, which is the text a reader would see.
ObjC.import("Foundation");

function read(p) {
  const s = $.NSString.stringWithContentsOfFileEncodingError(p, $.NSUTF8StringEncoding, null);
  if (!s || s.isNil()) throw new Error("cannot read " + p);
  return s.js;
}

// Each id in the static markup, mapped to the ids of the elements around it.
function ancestry(html) {
  const out = {}, stack = [];
  const re = /<(\/?)([a-zA-Z][\w-]*)([^>]*?)(\/?)>/g;
  const VOID = /^(meta|link|br|hr|img|input)$/i;
  let m;
  while ((m = re.exec(html))) {
    const [, close, tag, attrs, self] = m;
    if (close) { while (stack.length && stack.pop().tag !== tag) { /* unwind */ } continue; }
    const id = (/\bid="([^"]+)"/.exec(attrs) || [])[1] || null;
    if (id) out[id] = stack.filter(e => e.id).map(e => e.id);
    if (!self && !VOID.test(tag)) stack.push({ tag, id });
  }
  return out;
}

function run(argv) {
  const [page, mode, showAll, msPref, dataPath] = argv;
  const html = read(page);
  const at = html.lastIndexOf("<script>");
  const markup = html.slice(0, at);
  const src = html.slice(at + 8, html.lastIndexOf("</script>"));
  const data = dataPath ? JSON.parse(read(dataPath)) : null;

  // Elements start as the static markup has them: hidden if it says so.
  const parents = ancestry(markup);
  const startHidden = id => new RegExp(`id="${id}"[^>]*\\shidden\\b`).test(markup);
  const els = {};
  const el = id => (els[id] = els[id] || {
    id, innerHTML: "", textContent: "", hidden: startHidden(id), value: "", onclick: null, onchange: null,
    querySelectorAll: () => [], addEventListener: () => {}, matches: () => false,
    classList: { add() {}, remove() {}, toggle() {} } });
  const store = {};
  if (showAll !== "-") store["marketlake-board-all"] = showAll;
  if (msPref !== "-") store["marketlake-board-ms"] = msPref;
  const timers = [];
  let onData = null, onError = null;
  const db = { doc: () => ({ onSnapshot: (cb, err) => { onData = cb; onError = err; } }) };
  const window = mode === "none" ? {} : { claude: { use: () => new Promise(res => { if (mode !== "loading") res(db); }) } };
  const document = { getElementById: el, querySelectorAll: () => [], addEventListener: () => {}, removeEventListener: () => {} };
  const localStorage = { getItem: k => (k in store ? store[k] : null), setItem: (k, v) => { store[k] = String(v); } };

  const out = { mode, errors: [] };
  const guard = (what, f) => { try { f(); } catch (e) { out.errors.push(what + ": " + e); } };
  guard("load", () => new Function("document", "localStorage", "setTimeout", "setInterval", "window", src)(
    document, localStorage, f => { timers.push(f); return 0; }, () => 0, window));
  // osascript runs promise callbacks only after run() returns, so the page's
  // async subscription and the rest of this check run as callbacks too, after
  // enough turns for the page to reach onSnapshot.
  let p = Promise.resolve();
  for (let i = 0; i < 20; i++) p = p.then(() => 0);
  p.then(finish).catch(e => print(JSON.stringify({ errors: ["harness: " + e] })));

  function finish() {
    const snap = { data: () => data, exists: true };
    if (mode === "empty") guard("snapshot", () => onData({ exists: false }));
    if (mode === "shape") guard("snapshot", () => onData({ exists: true, data: () => ({ schema: 1 }) }));
    if (mode === "stopped") guard("error", () => onError({ code: "permission-denied" }));
    if (mode === "live" || mode === "dropped") guard("snapshot", () => onData(snap));
    if (mode === "dropped") guard("error", () => onError({ code: "unavailable" }));
    timers.forEach(f => guard("timer", f)); // as if every timeout had elapsed
    out.subscribed = !!onData;
    out.timers = timers.length;

    const strip = h => h.replace(/<[^>]+>/g, " ").replace(/&amp;/g, "&").replace(/&lt;/g, "<")
      .replace(/&gt;/g, ">").replace(/&quot;/g, '"').replace(/\s+/g, " ").trim();
    const drawn = id => !(els[id] || { hidden: startHidden(id) }).hidden;
    out.elements = {};
    out.visible = {};
    Object.keys(els).sort().forEach(id => {
      const e = els[id];
      out.elements[id] = { html: e.innerHTML, text: e.textContent, hidden: e.hidden, value: e.value };
      const shown = drawn(id) && (parents[id] || []).every(drawn);
      const t = strip(e.innerHTML) || e.textContent;
      if (shown && t) out.visible[id] = t;
    });
    print(JSON.stringify(out, null, 1));
  }
}

function print(s) {
  $.NSFileHandle.fileHandleWithStandardOutput.writeData($(s + "\n").dataUsingEncoding($.NSUTF8StringEncoding));
}
