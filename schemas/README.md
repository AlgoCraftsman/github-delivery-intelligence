# Event schemas

`github-event-envelope-v1.json` is the checked-in JSON Schema for the raw Kafka
event contract. The outer envelope is closed and versioned. The nested `payload`
object deliberately accepts unknown fields so additive GitHub payload changes do
not break ingestion.
