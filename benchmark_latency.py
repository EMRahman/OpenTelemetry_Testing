import asyncio
import json
import time
import statistics
import websockets
from opentelemetry import _logs
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import SimpleLogRecordProcessor
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.sdk.resources import Resource, SERVICE_NAME

# Setup OTel Logger
resource = Resource.create({SERVICE_NAME: "latency-benchmark-worker"})
exporter = OTLPLogExporter(endpoint="http://127.0.0.1:4318/v1/logs")
provider = LoggerProvider(resource=resource)
provider.add_log_record_processor(SimpleLogRecordProcessor(exporter))
_logs.set_logger_provider(provider)

import logging
handler = LoggingHandler(level=logging.INFO, logger_provider=provider)
logger = logging.getLogger("benchmark_producer")
logger.setLevel(logging.INFO)
logger.addHandler(handler)

NUM_EVENTS = 15

async def benchmark():
    ws_uri = "ws://127.0.0.1:4318/ws"
    print(f"Connecting GUI client WebSocket to {ws_uri}...")
    
    async with websockets.connect(ws_uri) as ws:
        # Read init message
        init_msg = await ws.recv()
        print("Connected to WebSocket successfully. Beginning latency benchmark run...")
        print("-" * 75)

        test_cases = [
            ("WARNING", "Degraded p99 latency warning", logging.WARNING),
            ("CRITICAL", "Database connection pool timeout", logging.ERROR),
            ("WARNING", "High memory pressure threshold exceeded", logging.WARNING),
            ("OK", "Nominal state restored", logging.INFO),
            ("CRITICAL", "Unhandled exception in background worker task", logging.ERROR),
            ("WARNING", "Slow response time on upstream dependency", logging.WARNING),
            ("CRITICAL", "Service mesh circuit breaker tripped", logging.ERROR),
            ("OK", "Health checks passing", logging.INFO),
            ("WARNING", "Queue depth above 85% threshold", logging.WARNING),
            ("CRITICAL", "Fatal error: Out of memory on child worker", logging.ERROR),
            ("WARNING", "Disk I/O latency spike", logging.WARNING),
            ("CRITICAL", "Kafka broker connection lost", logging.ERROR),
            ("OK", "Nominal state restored", logging.INFO),
            ("WARNING", "Elevated CPU utilization 92%", logging.WARNING),
            ("CRITICAL", "Deadlock detected in transactional database", logging.ERROR),
        ]

        results = []

        for idx, (status, msg, log_level) in enumerate(test_cases, 1):
            t_event_occurred = time.perf_counter()
            emitted_ms = time.time() * 1000

            # Emit via OTel SDK
            logger.log(
                log_level,
                msg,
                extra={
                    "state.status": status,
                    "benchmark.id": idx,
                    "emitted_at_ms": emitted_ms
                }
            )
            t_otel_dispatched = time.perf_counter()
            sdk_dispatch_latency = (t_otel_dispatched - t_event_occurred) * 1000

            # Wait for the WebSocket to deliver the event to the GUI
            raw_ws_msg = await ws.recv()
            t_gui_received = time.perf_counter()
            e2e_latency = (t_gui_received - t_event_occurred) * 1000

            payload = json.loads(raw_ws_msg)
            current_state = payload.get("current_state", {})
            ingest_latency = current_state.get("ingest_latency_ms", 0.0)

            results.append({
                "idx": idx,
                "status": status,
                "msg": msg,
                "sdk_dispatch_ms": sdk_dispatch_latency,
                "ingest_ms": ingest_latency,
                "e2e_ms": e2e_latency
            })

            symbol = "🟡" if status == "WARNING" else ("🔴" if status == "CRITICAL" else "🟢")
            print(f"[{idx:02d}] {symbol} {status:8} | E2E to GUI: {e2e_latency:6.2f} ms | (OTel SDK export: {sdk_dispatch_latency:5.2f} ms | Consumer ingest: {ingest_latency:5.2f} ms) | {msg}")
            
            # Small delay between events to simulate real transitions
            await asyncio.sleep(0.4)

        print("-" * 75)
        print("\n📊 STATISTICAL ANALYSIS (Time from Error/Warning in Process -> Rendered on GUI):")
        
        # Analyze Warning & Critical events specifically
        alert_events = [r for r in results if r["status"] in ["WARNING", "CRITICAL"]]
        e2e_times = [r["e2e_ms"] for r in alert_events]
        sdk_times = [r["sdk_dispatch_ms"] for r in alert_events]
        ingest_times = [r["ingest_ms"] for r in alert_events]

        print(f"Total Alert Events Sampled: {len(alert_events)}")
        print(f"Mean End-to-End Latency:    {statistics.mean(e2e_times):.2f} ms")
        print(f"Median (p50) Latency:       {statistics.median(e2e_times):.2f} ms")
        print(f"Min Latency:                {min(e2e_times):.2f} ms")
        print(f"Max Latency:                {max(e2e_times):.2f} ms")
        print(f"Std Deviation:              {statistics.stdev(e2e_times):.2f} ms")
        print(f"OTel SDK Serializer/HTTP:   Mean {statistics.mean(sdk_times):.2f} ms")
        print(f"Consumer Protobuf Ingest:   Mean {statistics.mean(ingest_times):.2f} ms")
        print("-" * 75)

    provider.shutdown()

if __name__ == "__main__":
    asyncio.run(benchmark())
