import os
import logging

logger = logging.getLogger("chainpulse")


def setup_telemetry(app):
    try:
        from opentelemetry import trace
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import (
            BatchSpanProcessor,
            ConsoleSpanExporter,
        )
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

        resource = Resource.create(
            {
                "service.name": "chainpulse-api",
                "service.version": os.getenv("MODEL_VERSION", "5.0.0"),
                "deployment.environment": (
                    "production" if os.getenv("RENDER") else "development"
                ),
            }
        )

        provider = TracerProvider(resource=resource)
        provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
        trace.set_tracer_provider(provider)

        FastAPIInstrumentor.instrument_app(
            app,
            excluded_urls="health,docs,openapi.json",
        )

        logger.info("OpenTelemetry tracing enabled")

    except ImportError:
        logger.warning("OpenTelemetry not installed - tracing disabled")
    except Exception as e:
        logger.warning(f"Telemetry setup failed: {e}")
