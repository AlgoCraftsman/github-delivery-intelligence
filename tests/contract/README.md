# Contract tests

`test_github_event_envelope.py` validates every sanitized MVP fixture against both
the Pydantic envelope and the checked-in JSON Schema. It also locks the version,
required fields, outer-envelope strictness, and additive payload compatibility.
