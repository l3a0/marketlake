// A DOM small enough to run src/lake/static/status.html's script, with a real element
// tree. tests/component/test_status_page_cost.py loads it ahead of the page's script.
//
// The build board's stub, now in l3a0/repo-template, keeps only an innerHTML string
// per id, because the board's page writes markup as strings. status.html builds nodes
// with createElement and createElementNS and appends them, so this stub keeps the
// nodes, and `__dump(node)` hands a test the tree the page drew.
//
// Plain ES5 that assumes nothing about its host, so `node` and `osascript -l
// JavaScript` both run it. An element the page asks for by id is created on demand,
// as the board's stub does, since the page's static markup is not parsed here.
// `__WIDTH` is the width every element reports, which the page's charts size to, unless
// `__WIDTHS` names a width for that element's id.
var __WIDTH = typeof __WIDTH === "number" ? __WIDTH : 640;
var __WIDTHS = typeof __WIDTHS === "object" ? __WIDTHS : {};

function __Node(tag, ns) {
  this.tagName = tag;
  this.namespaceURI = ns || null;
  this.childNodes = [];
  this.parentNode = null;
  this.attributes = {};
  this.listeners = {};
  this.style = {};
  this._class = "";
  this.title = "";
  this.hidden = false;
  this.open = false;
  this._text = "";
  var self = this;
  this.classList = {
    add: function (name) {
      var names = self._class ? self._class.split(/\s+/) : [];
      if (names.indexOf(name) < 0) { names.push(name); }
      self._class = names.join(" ");
    },
    remove: function (name) {
      self._class = (self._class ? self._class.split(/\s+/) : [])
        .filter(function (n) { return n !== name; }).join(" ");
    },
    toggle: function (name, on) {
      var has = self.classList.contains(name);
      if (on === undefined) { on = !has; }
      if (on) { self.classList.add(name); } else { self.classList.remove(name); }
      return on;
    },
    contains: function (name) {
      return (self._class ? self._class.split(/\s+/) : []).indexOf(name) >= 0;
    }
  };
}
// An SVG element's ``className`` is an SVGAnimatedString in a browser, and assigning it
// throws under the page's "use strict", so here too it can be read and never assigned.
Object.defineProperty(__Node.prototype, "className", {
  get: function () { return this._class; },
  set: function (value) {
    if (this.namespaceURI) { throw new TypeError("className is read-only on an SVG element"); }
    this._class = String(value);
  }
});
Object.defineProperty(__Node.prototype, "firstChild", {
  get: function () { return this.childNodes.length ? this.childNodes[0] : null; }
});
Object.defineProperty(__Node.prototype, "lastChild", {
  get: function () {
    return this.childNodes.length ? this.childNodes[this.childNodes.length - 1] : null;
  }
});
Object.defineProperty(__Node.prototype, "children", {
  get: function () { return this.childNodes.filter(function (n) { return n.tagName !== "#text"; }); }
});
Object.defineProperty(__Node.prototype, "clientWidth", {
  get: function () {
    return Object.prototype.hasOwnProperty.call(__WIDTHS, this.id) ? __WIDTHS[this.id] : __WIDTH;
  }
});
Object.defineProperty(__Node.prototype, "textContent", {
  get: function () {
    if (this.tagName === "#text") { return this._text; }
    return this.childNodes.map(function (n) { return n.textContent; }).join("");
  },
  set: function (value) {
    if (this.tagName === "#text") { this._text = String(value); return; }
    this.childNodes.forEach(function (n) { n.parentNode = null; });
    this.childNodes = [];
    if (value !== "" && value !== null && value !== undefined) {
      this.appendChild(document.createTextNode(String(value)));
    }
  }
});
__Node.prototype.appendChild = function (child) {
  if (child.parentNode) { child.parentNode.removeChild(child); }
  child.parentNode = this;
  this.childNodes.push(child);
  return child;
};
__Node.prototype.insertBefore = function (child, before) {
  if (!before) { return this.appendChild(child); }
  if (child.parentNode) { child.parentNode.removeChild(child); }
  var index = this.childNodes.indexOf(before);
  if (index < 0) { throw new Error("insertBefore: not a child"); }
  child.parentNode = this;
  this.childNodes.splice(index, 0, child);
  return child;
};
__Node.prototype.removeChild = function (child) {
  var index = this.childNodes.indexOf(child);
  if (index < 0) { throw new Error("removeChild: not a child"); }
  this.childNodes.splice(index, 1);
  child.parentNode = null;
  return child;
};
__Node.prototype.setAttribute = function (name, value) {
  this.attributes[name] = String(value);
  if (name === "class") { this._class = String(value); }
};
__Node.prototype.getAttribute = function (name) {
  if (name === "class") { return this.className || null; }
  return Object.prototype.hasOwnProperty.call(this.attributes, name)
    ? this.attributes[name] : null;
};
__Node.prototype.removeAttribute = function (name) { delete this.attributes[name]; };
__Node.prototype.addEventListener = function (type, fn) {
  (this.listeners[type] = this.listeners[type] || []).push(fn);
};
__Node.prototype.removeEventListener = function (type, fn) {
  this.listeners[type] = (this.listeners[type] || []).filter(function (f) { return f !== fn; });
};
__Node.prototype.dispatch = function (type, event) {
  var self = this;
  event = event || {};
  event.type = type;
  event.target = event.target || self;
  (self.listeners[type] || []).forEach(function (fn) { fn.call(self, event); });
};
__Node.prototype.getBoundingClientRect = function () {
  return { left: 0, top: 0, right: __WIDTH, bottom: 0, width: __WIDTH, height: 0 };
};
__Node.prototype.querySelectorAll = function () { return []; };

var __byId = {};
var document = {
  body: new __Node("body"),
  getElementById: function (id) {
    if (!__byId[id]) {
      var node = new __Node("div");
      node.id = id;
      __byId[id] = node;
    }
    return __byId[id];
  },
  createElement: function (tag) { return new __Node(String(tag).toLowerCase()); },
  createElementNS: function (ns, tag) { return new __Node(String(tag), ns); },
  createTextNode: function (text) {
    var node = new __Node("#text");
    node._text = String(text);
    return node;
  },
  querySelectorAll: function () { return []; },
  addEventListener: function () {},
  removeEventListener: function () {}
};
var window = {
  listeners: {},
  addEventListener: function (type, fn) {
    (this.listeners[type] = this.listeners[type] || []).push(fn);
  },
  removeEventListener: function () {},
  innerWidth: __WIDTH
};

// The tree under a node as plain data, for the driver to print as JSON.
function __dump(node) {
  if (node.tagName === "#text") { return { tag: "#text", text: node._text }; }
  var out = { tag: node.tagName, ns: node.namespaceURI, cls: node.className, attrs: node.attributes,
    title: node.title, style: node.style, open: node.open, children: [] };
  if (node.id) { out.id = node.id; }
  node.childNodes.forEach(function (child) { out.children.push(__dump(child)); });
  return out;
}
