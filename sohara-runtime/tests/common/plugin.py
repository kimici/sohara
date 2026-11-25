import json
import os
import sys

source_done = False
trigger_started = False
trigger_done = False

def read_store(path):
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)

def write_store(path, data):
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(data, handle)

for line in sys.stdin:
    request = json.loads(line)
    method = request["method"]
    req_id = request["id"]
    params = request.get("params", {})
    if method == "initialize":
        response = {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "protocol": "sohara.stdio/v1",
                "capabilities": {
                    "source": True,
                    "transform": True,
                    "sink": True,
                    "trigger": True,
                    "state_store": True,
                    "event_bus": True,
                },
            },
        }
    elif method == "source.pull":
        if source_done:
            response = {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"records": [], "done": True},
            }
        else:
            source_done = True
            response = {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "records": [
                        {
                            "id": "remote-1",
                            "timestamp": "2026-01-01T00:00:00Z",
                            "payload": {"name": "remote-a"},
                            "metadata": {},
                        },
                        {
                            "id": "remote-2",
                            "timestamp": "2026-01-01T00:00:01Z",
                            "payload": {"name": "remote-b"},
                            "metadata": {},
                        },
                    ],
                    "done": True,
                },
            }
    elif method == "transform":
        record = params["record"]
        payload = record["payload"]
        field = params["config"].get("field", "name")
        value = payload.get(field, "")
        if not value:
            response = {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"outcome": "filtered"},
            }
        else:
            payload[field] = str(value).upper()
            response = {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"outcome": "pass", "record": record},
            }
    elif method == "sink.send":
        path = params["config"]["path"]
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(params["record"]["payload"]) + "\n")
        response = {"jsonrpc": "2.0", "id": req_id, "result": {}}
    elif method == "sink.flush":
        response = {"jsonrpc": "2.0", "id": req_id, "result": {}}
    elif method == "trigger.start":
        trigger_started = True
        response = {"jsonrpc": "2.0", "id": req_id, "result": {}}
    elif method == "trigger.pull":
        if not trigger_started or trigger_done:
            response = {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"records": [], "done": True},
            }
        else:
            trigger_done = True
            response = {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "records": [
                        {
                            "id": "trigger-1",
                            "timestamp": "2026-01-01T00:00:02Z",
                            "payload": {"tick": 1},
                            "metadata": {},
                        },
                        {
                            "id": "trigger-2",
                            "timestamp": "2026-01-01T00:00:03Z",
                            "payload": {"tick": 2},
                            "metadata": {},
                        },
                    ],
                    "done": True,
                },
            }
    elif method == "trigger.stop":
        trigger_done = True
        response = {"jsonrpc": "2.0", "id": req_id, "result": {}}
    elif method == "state.load":
        path = params["config"]["path"]
        data = read_store(path)
        key = params["key"]
        if key in data:
            response = {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"found": True, "value": data[key]},
            }
        else:
            response = {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"found": False, "value": None},
            }
    elif method == "state.save":
        path = params["config"]["path"]
        data = read_store(path)
        data[params["key"]] = params["value"]
        write_store(path, data)
        response = {"jsonrpc": "2.0", "id": req_id, "result": {}}
    elif method == "state.delete":
        path = params["config"]["path"]
        data = read_store(path)
        data.pop(params["key"], None)
        write_store(path, data)
        response = {"jsonrpc": "2.0", "id": req_id, "result": {}}
    elif method == "bus.publish":
        path = params["config"]["path"]
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(
                json.dumps(
                    {"topic": params["topic"], "payload": params["payload"]}
                )
                + "\n"
            )
        response = {"jsonrpc": "2.0", "id": req_id, "result": {}}
    else:
        response = {
            "jsonrpc": "2.0",
            "id": req_id,
            "error": {"code": -32601, "message": f"unknown method: {method}"},
        }
    sys.stdout.write(json.dumps(response) + "\n")
    sys.stdout.flush()
