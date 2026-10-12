import asyncio
import time
import json
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

from fastapi import FastAPI, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from opentelemetry.proto.collector.logs.v1 import logs_service_pb2
from opentelemetry.proto.common.v1 import common_pb2

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("dashboard_consumer")

app = FastAPI(title="OTel Health & State Dashboard")

# In-memory alert history & current state
state_lock = asyncio.Lock()
current_state: Dict[str, Any] = {
    "status": "UNKNOWN",
    "color": "#9ca3af",       # Gray initially
    "service_name": "worker-service",
    "message": "Waiting for telemetry events from producer...",
    "timestamp": datetime.now(timezone.utc).isoformat(),
    "attributes": {},
    "severity": "INFO"
}
event_history: List[Dict[str, Any]] = []
MAX_HISTORY = 50


class ConnectionManager:
    """Manages active WebSocket connections for real-time dashboard updates."""
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"Dashboard client connected. Total clients: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            logger.info(f"Dashboard client disconnected. Remaining clients: {len(self.active_connections)}")

    async def broadcast(self, message: dict):
        if not self.active_connections:
            return
        payload = json.dumps(message)
        dead_connections = []
        for connection in self.active_connections:
            try:
                await connection.send_text(payload)
            except Exception:
                dead_connections.append(connection)
        for dc in dead_connections:
            if dc in self.active_connections:
                self.active_connections.remove(dc)


ws_manager = ConnectionManager()


def extract_any_value(val: common_pb2.AnyValue) -> Any:
    """Extract native Python value from OpenTelemetry AnyValue protobuf."""
    field = val.WhichOneof("value")
    if field == "string_value":
        return val.string_value
    elif field == "bool_value":
        return val.bool_value
    elif field == "int_value":
        return val.int_value
    elif field == "double_value":
        return val.double_value
    elif field == "array_value":
        return [extract_any_value(v) for v in val.array_value.values]
    elif field == "kvlist_value":
        return {kv.key: extract_any_value(kv.value) for kv in val.kvlist_value.values}
    return None


def parse_otlp_log_request(body_bytes: bytes) -> List[Dict[str, Any]]:
    """Parse OTLP protobuf ExportLogsServiceRequest and extract alerts."""
    req = logs_service_pb2.ExportLogsServiceRequest.FromString(body_bytes)
    extracted_events = []

    for resource_log in req.resource_logs:
        resource_attrs = {
            kv.key: extract_any_value(kv.value)
            for kv in resource_log.resource.attributes
        }
        service_name = resource_attrs.get("service.name", "unknown-service")

        for scope_log in resource_log.scope_logs:
            for log_record in scope_log.log_records:
                # Extract log message/body
                body_field = log_record.body.WhichOneof("value")
                body_text = getattr(log_record.body, body_field) if body_field else ""

                # Extract attributes
                log_attrs = {
                    kv.key: extract_any_value(kv.value)
                    for kv in log_record.attributes
                }

                # Determine state status & color
                # Producer sends state in attributes ('state.status') or maps from severity
                state_status = log_attrs.get("state.status")
                severity_num = log_record.severity_number
                severity_text = log_record.severity_text or "INFO"

                if not state_status:
                    if severity_num >= 17:  # ERROR or FATAL
                        state_status = "CRITICAL"
                    elif severity_num >= 13: # WARN
                        state_status = "WARNING"
                    else:
                        state_status = "OK"

                # Normalize status and colors
                status_upper = str(state_status).upper()
                if status_upper in ["OK", "HEALTHY", "GREEN"]:
                    normalized_status = "OK"
                    color = "#22c55e" # Emerald Green
                elif status_upper in ["WARN", "WARNING", "DEGRADED", "YELLOW"]:
                    normalized_status = "WARNING"
                    color = "#eab308" # Amber / Yellow
                else:
                    normalized_status = "CRITICAL"
                    color = "#ef4444" # Red

                ts_ns = log_record.time_unix_nano or log_record.observed_time_unix_nano
                if ts_ns > 0:
                    dt = datetime.fromtimestamp(ts_ns / 1e9, tz=timezone.utc)
                    ts_iso = dt.isoformat()
                else:
                    ts_iso = datetime.now(timezone.utc).isoformat()

                event = {
                    "service_name": service_name,
                    "status": normalized_status,
                    "color": color,
                    "severity": severity_text,
                    "message": str(body_text),
                    "timestamp": ts_iso,
                    "attributes": {**resource_attrs, **log_attrs}
                }
                extracted_events.append(event)

    return extracted_events


@app.post("/v1/logs")
async def otlp_logs_receiver(request: Request):
    """
    Standard OTLP/HTTP Logs Endpoint (compliant with OpenTelemetry specs).
    Receives Protobuf payload from OpenTelemetry OTLPLogExporter.
    """
    body_bytes = await request.body()
    try:
        events = parse_otlp_log_request(body_bytes)
    except Exception as e:
        logger.error(f"Failed to parse OTLP protobuf log record: {e}", exc_info=True)
        # Still return successful 200 per OTLP receiver specification
        resp = logs_service_pb2.ExportLogsServiceResponse()
        return Response(content=resp.SerializeToString(), media_type="application/x-protobuf")

    recv_time_ms = round(time.time() * 1000, 3)
    global current_state, event_history
    async with state_lock:
        for event in events:
            # Measure consumer ingestion latency if emitter timestamp is present
            emitted_ms = event.get("attributes", {}).get("emitted_at_ms")
            if emitted_ms:
                latency_ms = round(recv_time_ms - float(emitted_ms), 2)
                event["ingest_latency_ms"] = max(0.0, latency_ms)
            else:
                event["ingest_latency_ms"] = 0.0

            current_state = event
            event_history.insert(0, event)
            if len(event_history) > MAX_HISTORY:
                event_history.pop()
            
            logger.info(
                f"[STATE EVENT] {event['service_name']} -> {event['status']} ({event['severity']}) | Ingest Latency: {event.get('ingest_latency_ms', 0)}ms | {event['message']}"
            )
            await ws_manager.broadcast({
                "type": "state_update",
                "current_state": current_state,
                "history_item": event
            })

    # Return valid empty OTLP protobuf ExportLogsServiceResponse
    response_pb = logs_service_pb2.ExportLogsServiceResponse()
    return Response(
        content=response_pb.SerializeToString(),
        media_type="application/x-protobuf"
    )


@app.get("/api/state")
async def get_state():
    """REST endpoint for initial client state hydration."""
    async with state_lock:
        return {
            "current_state": current_state,
            "history": event_history
        }


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint pushing real-time state alerts to browser."""
    await ws_manager.connect(websocket)
    # Immediately send the latest state upon connection
    async with state_lock:
        await websocket.send_text(json.dumps({
            "type": "init",
            "current_state": current_state,
            "history": event_history
        }))
    try:
        while True:
            # Keep connection alive; handle potential client pings
            _ = await websocket.receive_text()
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)
    except Exception:
        ws_manager.disconnect(websocket)


@app.get("/", response_class=HTMLResponse)
async def dashboard_page():
    """Serves the real-time status dashboard web page."""
    with open("dashboard/index.html", "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read())


@app.get("/architecture", response_class=HTMLResponse)
async def architecture_page():
    """Serves the interactive Architecture & 10k Server Scale web page."""
    with open("dashboard/architecture.html", "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read())


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("dashboard.consumer:app", host="127.0.0.1", port=4318, reload=False)
