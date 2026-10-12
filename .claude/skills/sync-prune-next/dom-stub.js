// A DOM small enough to run board.html's script, plus a no-op setInterval so the
// page's timers do not keep the process alive. It is a port of the stub
// l3a0/quantitative-trading uses to test its own board.
// Here tests/component/test_board_page.py loads it, serves the page fake fetch
// answers, and reads what the page drew.
//
// A parse check does not catch the defects the board ships. A sentence that
// renders perfectly can still say something false, so the check is to execute
// the script and read what it produced.
//
// This is plain ES5 and assumes nothing about its host, so `node` and
// `osascript -l JavaScript` both run it. Every element the script asks for is
// created on demand and kept in `store`, so a driver appended after the script
// can read `store.<id>` for any id the page renders into. An element starts
// hidden when the page's static markup says `hidden`, as in a browser. The
// driver names those ids in `__STARTS_HIDDEN` before this file runs.
var store = {};
function mkEl(id) {
  var hidden = typeof __STARTS_HIDDEN !== "undefined" && __STARTS_HIDDEN.indexOf(id) >= 0;
  return { id: id, innerHTML: "", textContent: "", value: "", hidden: hidden,
    classList: { add: function(){}, remove: function(){}, toggle: function(){} },
    querySelectorAll: function(){ return []; },
    onclick: null, onkeydown: null, onchange: null };
}
var document = {
  getElementById: function(id){ if(!store[id]) store[id]=mkEl(id); return store[id]; },
  addEventListener: function(){},
  // Each redraw after the first removes the Escape listener the previous one
  // added, so a stub without this throws on the second draw.
  removeEventListener: function(){} };
var setInterval = function(){ return 0; };
