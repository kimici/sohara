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

function __deepClone(value) {
  return JSON.parse(JSON.stringify(value));
}

function __flowStep(kind, id, type, config) {
  return {
    id: String(id),
    kind: String(kind),
    type: String(type),
    config: config === undefined ? {} : __deepClone(config)
  };
}

function __FlowBuilder(name) {
  this._flow = {
    name: String(name),
    version: "1",
    steps: [],
    edges: [],
    triggers: [],
    vars: {}
  };
}
__FlowBuilder.prototype.version = function(version) {
  this._flow.version = String(version);
  return this;
};
__FlowBuilder.prototype.description = function(text) {
  this._flow.description = String(text);
  return this;
};
__FlowBuilder.prototype.source = function(id, type, config) {
  this._flow.steps.push(__flowStep("source", id, type, config));
  return this;
};
__FlowBuilder.prototype.transform = function(id, type, config) {
  this._flow.steps.push(__flowStep("transform", id, type, config));
  return this;
};
__FlowBuilder.prototype.sink = function(id, type, config) {
  this._flow.steps.push(__flowStep("sink", id, type, config));
  return this;
};
__FlowBuilder.prototype.control = function(id, type, config) {
  this._flow.steps.push(__flowStep("control", id, type, config));
  return this;
};
__FlowBuilder.prototype.edge = function(from, to, when) {
  var edge = { from: String(from), to: String(to) };
  if (when !== undefined) edge.when = String(when);
  this._flow.edges.push(edge);
  return this;
};
__FlowBuilder.prototype.trigger = function(id, type, config) {
  this._flow.triggers.push({
    id: String(id),
    type: String(type),
    config: config === undefined ? {} : __deepClone(config)
  });
  return this;
};
__FlowBuilder.prototype.vars = function(vars) {
  this._flow.vars = vars === undefined ? {} : __deepClone(vars);
  return this;
};
__FlowBuilder.prototype.checkpoint = function(config) {
  this._flow.checkpoint = config === undefined ? {} : __deepClone(config);
  return this;
};
__FlowBuilder.prototype.eventBus = function(type, config) {
  if (typeof type === "string") {
    var bus = config === undefined ? {} : __deepClone(config);
    bus.type = String(type);
    this._flow.event_bus = bus;
  } else {
    this._flow.event_bus = type === undefined ? undefined : __deepClone(type);
  }
  return this;
};
__FlowBuilder.prototype.build = function() {
  var built = __deepClone(this._flow);
  globalThis.__lastBuiltFlow = built;
  return built;
};
__FlowBuilder.prototype.save = function(path) {
  var built = this.build();
  __flow_save(built, String(path));
  return built;
};

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
  },
  registerStep: function(kind, type, handler) {
    if (typeof handler !== "function") {
      throw new Error("sohara.registerStep(kind, type, handler): handler must be a function");
    }
    globalThis.__registeredStepSeq = (globalThis.__registeredStepSeq || 0) + 1;
    var handlerName = "__registered_step_" + globalThis.__registeredStepSeq;
    globalThis[handlerName] = handler;
    return __register_step(String(kind), String(type), handlerName);
  }
};
sohara.flow = function(name) { return new __FlowBuilder(name); };
sohara.flow.save = function(config, path) {
  globalThis.__lastBuiltFlow = config;
  return __flow_save(config, String(path));
};
sohara.flow.run = function(config) {
  var handleId = __flow_run(config);
  var stopped = false;
  return {
    stop: function() {
      if (!stopped) {
        __flow_stop(String(handleId));
        stopped = true;
      }
    }
  };
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
