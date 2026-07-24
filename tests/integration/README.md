# Integration tests

`test_webhook_kafka_outage.py` always runs against a deliberately closed local port and
proves a broker outage produces a bounded non-`2xx` HTTP response.

`test_webhook_kafka_live.py` is opt-in because it requires the local Compose Kafka
broker. Start the services and run it with:

```bash
RUN_KAFKA_INTEGRATION=1 uv run pytest tests/integration/test_webhook_kafka_live.py
```
