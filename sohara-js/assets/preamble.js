// Host bridge injected into every script context (see docs/design/quickjs-api.md).
// Sync host calls (baseline per §7); async bridge is not part of the MVP.

var __RecordProto = {
  get: function(path) {
    if (String(path).indexOf(".") < 0) return this[path];
    var parts = String(path).split(".");
    var value = this;
    for (var i = 0; i < parts.length; i++) {
      if (value == null) return undefined;
      value = value[parts[i]];
    }
    return value;
  },
  set: function(path, value) {
    var parts = String(path).split(".");
    var node = this;
    for (var i = 0; i < parts.length - 1; i++) {
      node = node[parts[i]] || (node[parts[i]] = {});
    }
    node[parts[parts.length - 1]] = value;
  },
  has: function(path) { return this.get(path) !== undefined; },
  unset: function(path) {
    var parts = String(path).split(".");
    var node = this;
    for (var i = 0; i < parts.length - 1; i++) {
      node = node[parts[i]];
      if (node == null) return;
    }
    delete node[parts[parts.length - 1]];
  },
  toJSON: function() {
    var out = {};
    for (var key in this) {
      if (this.hasOwnProperty(key)) out[key] = this[key];
    }
    return out;
  }
};
Object.defineProperty(__RecordProto, "id", {
  get: function() { return this.__meta && this.__meta.id; }, enumerable: false
});
Object.defineProperty(__RecordProto, "timestamp", {
  get: function() { return this.__meta && this.__meta.timestamp; }, enumerable: false
});
Object.defineProperty(__RecordProto, "schema", {
  get: function() { return null; }, enumerable: false
});
Object.defineProperty(__RecordProto, "metadata", {
  get: function() { return this.__meta && this.__meta.metadata; }, enumerable: false
});

function __wrapRecord(payload, meta) {
  var record = Object.create(__RecordProto);
  Object.assign(record, payload);
  if (meta) {
    Object.defineProperty(record, "__meta", { value: meta, enumerable: false });
  }
  return record;
}

var sohara = {
  log: function(level, msg) { return __log(String(level), String(msg)); },
  env: function(name, fallback) {
    var value = __env(String(name));
    return value === undefined ? fallback : value;
  },
  var: function(name, fallback) {
    var value = __var(String(name));
    return value === undefined ? fallback : value;
  },
  now: function() { return __now(); },
  uuid: function() { return __uuid(); },
  fail: function(msg) { throw new Error(msg === undefined ? "script failed" : String(msg)); },
  json: JSON,
  record: function(data) { return __wrapRecord(data === undefined ? {} : data, null); },
  sleep: function(ms) { return __sleep(Number(ms)); },
  notify: function(topic, payload) { return __notify(String(topic), payload); },
  file: {
    read: function(path) { return __file_read(String(path)); },
    write: function(path, content) { return __file_write(String(path), String(content)); }
  },
  http: {
    request: function(opts) { return __http_request(opts === undefined ? {} : opts); }
  },
  db: {
    query: function(sql, params) {
      return __db_query(String(sql), params === undefined ? [] : params);
    }
  }
};

// CommonJS-style module loader (§6): per-context cache.
var __requireCache = new Map();
function require(path) {
  var id = String(path);
  if (id === "sohara") return sohara;
  if (__requireCache.has(id)) return __requireCache.get(id);
  var source = __require_source(id);
  var module = { exports: {} };
  var factory = new Function("module", "exports", "require", source);
  factory(module, module.exports, require);
  __requireCache.set(id, module.exports);
  return module.exports;
}

// Per-invocation context (§5).
function __makeCtx(stepMeta, flowMeta, state, correlationId) {
  return {
    step: stepMeta,
    flow: flowMeta,
    state: state,
    correlation_id: correlationId,
    log: sohara.log,
    fail: sohara.fail,
    env: sohara.env,
    var: sohara.var,
    emit: function(record) { return __emit(record); },
    checkpoint: function() { return __checkpoint(); }
  };
}

// Entry invocations: wrap records, build ctx, sync state back after the call.
function __call2(entryName, recordPayload, stepMeta, flowMeta, state, correlationId, recordMeta) {
  var ctx = __makeCtx(stepMeta, flowMeta, state, correlationId);
  var meta = recordMeta || { id: ctx.step.id + "-" + correlationId, timestamp: __now() };
  var record = __wrapRecord(recordPayload, meta);
  var entry = globalThis[String(entryName)];
  if (typeof entry !== "function") {
    throw new Error("script entry '" + entryName + "' is not a function");
  }
  var result = entry(record, ctx);
  __state_sync(state);
  return result;
}

function __call1(entryName, stepMeta, flowMeta, state, correlationId) {
  var ctx = __makeCtx(stepMeta, flowMeta, state, correlationId);
  var entry = globalThis[String(entryName)];
  if (typeof entry !== "function") {
    throw new Error("script entry '" + entryName + "' is not a function");
  }
  var result = entry(ctx);
  __state_sync(state);
  return result;
}

// Backwards-compatible global (was the only ctx in S5).
globalThis.__ctx = __makeCtx({ id: "script" }, {}, {}, "");
