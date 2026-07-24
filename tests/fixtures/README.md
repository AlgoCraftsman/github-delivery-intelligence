# Sanitized GitHub fixtures

The JSON files in this directory are synthetic examples for the five MVP webhook
families. Repository names, user names, URLs, commit SHAs, delivery identifiers,
and numeric identifiers are invented and do not refer to a private repository or
person.

Fixtures retain the common GitHub payload shape and a representative
event-specific object. They intentionally include `fixture_extension` to prove
that unknown payload fields remain compatible with the event envelope.

When adding a fixture:

1. Replace all organization, repository, user, email, URL, token, and identifier
   values with obvious synthetic data.
2. Remove fields that are not needed to exercise the contract.
3. Search the file for source organization and repository names before commit.
4. Never include webhook signatures or secrets; tests compute signatures at
   runtime.
