import time
import random
import logging
from opentelemetry import _logs
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import SimpleLogRecordProcessor
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.sdk.resources import Resource, SERVICE_NAME, SERVICE_VERSION, DEPLOYMENT_ENVIRONMENT

# 1. Initialize OpenTelemetry Resource
resource = Resource.create({
    SERVICE_NAME: "order-worker-service",
    SERVICE_VERSION: "1.0.0",
    DEPLOYMENT_ENVIRONMENT: "production",
    "host.name": "local-macos",
    "process.runtime": "cpython"
})

# 2. Configure OTel Logger Provider and OTLP/HTTP Exporter
# By default, OTLPLogExporter sends to http://127.0.0.1:4318/v1/logs using protobuf
exporter = OTLPLogExporter(endpoint="http://127.0.0.1:4318/v1/logs")
provider = LoggerProvider(resource=resource)
# SimpleLogRecordProcessor delivers each event immediately without batching delays
provider.add_log_record_processor(SimpleLogRecordProcessor(exporter))
_logs.set_logger_provider(provider)

# 3. Attach OpenTelemetry LoggingHandler to standard Python logging
handler = LoggingHandler(level=logging.INFO, logger_provider=provider)
logger = logging.getLogger("worker_producer")
logger.setLevel(logging.INFO)
logger.addHandler(handler)

# Also log to local stdout console for operator visibility
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter("[PRODUCER] %(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(console_handler)

STATES = ["OK", "WARNING", "CRITICAL"]

def simulate_event():
    """
    Selects a simulated system state (Green/Yellow/Red) and emits
    an OpenTelemetry log record with semantic status and telemetry attributes.
    """
    # Weighted random selection: 60% OK, 25% WARNING, 15% CRITICAL
    status = random.choices(STATES, weights=[0.60, 0.25, 0.15], k=1)[0]

    # High-precision monotonic timestamp for latency benchmarking
    emitted_at_ms = round(time.time() * 1000, 3)

    if status == "OK":
        memory_usage_pct = random.uniform(25.0, 55.0)
        latency_ms = random.uniform(15.0, 95.0)
        extra_attrs = {
            "state.status": "OK",
            "system.memory.usage_pct": round(memory_usage_pct, 1),
            "http.server.duration_ms": round(latency_ms, 1),
            "worker.active_tasks": random.randint(3, 12),
            "emitted_at_ms": emitted_at_ms
        }
        logger.info(
            f"Nominal operation: All worker health checks passed (latency: {extra_attrs['http.server.duration_ms']}ms)",
            extra=extra_attrs
        )

    elif status == "WARNING":
        memory_usage_pct = random.uniform(75.0, 89.0)
        latency_ms = random.uniform(400.0, 850.0)
        extra_attrs = {
            "state.status": "WARNING",
            "system.memory.usage_pct": round(memory_usage_pct, 1),
            "http.server.duration_ms": round(latency_ms, 1),
            "alert.threshold": "high_latency_warning",
            "emitted_at_ms": emitted_at_ms
        }
        logger.warning(
            f"Degraded performance: High p99 response time ({extra_attrs['http.server.duration_ms']}ms) & memory pressure ({extra_attrs['system.memory.usage_pct']}%)",
            extra=extra_attrs
        )

    elif status == "CRITICAL":
        extra_attrs = {
            "state.status": "CRITICAL",
            "error.type": "DatabaseConnectionTimeout",
            "error.code": 503,
            "retry_count": 3,
            "emitted_at_ms": emitted_at_ms
        }
        logger.error(
            "Service outage: Primary database connection pool exhausted, worker halted",
            extra=extra_attrs
        )

def main():
    print("=" * 65)
    print("🚀 OpenTelemetry State Event Producer Started")
    print("   Target OTLP Receiver: http://127.0.0.1:4318/v1/logs")
    print("   Emitting events every 3 seconds (Green -> Yellow -> Red)...")
    print("   Press Ctrl+C to stop.")
    print("=" * 65)

    try:
        while True:
            simulate_event()
            time.sleep(3)
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        provider.shutdown()

if __name__ == "__main__":
    main()
