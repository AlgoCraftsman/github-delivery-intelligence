CREATE TABLE IF NOT EXISTS serving.pull_request_projection_watermarks (
    repository_id bigint NOT NULL,
    pull_request_id bigint NOT NULL,
    last_event_at timestamptz NOT NULL,
    last_delivery_id text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (repository_id, pull_request_id),
    CONSTRAINT pull_request_watermark_repository_id_positive
        CHECK (repository_id > 0),
    CONSTRAINT pull_request_watermark_pull_request_id_positive
        CHECK (pull_request_id > 0),
    CONSTRAINT pull_request_watermark_delivery_id_nonempty
        CHECK (btrim(last_delivery_id) <> '')
);

COMMENT ON TABLE serving.pull_request_projection_watermarks IS
    'Latest source snapshot applied per pull request, including closed-state tombstones.';

CREATE TABLE IF NOT EXISTS serving.pull_request_first_reviews (
    repository_id bigint NOT NULL,
    pull_request_id bigint NOT NULL,
    review_id bigint NOT NULL,
    reviewer_id bigint NOT NULL,
    reviewer_login text NOT NULL,
    submitted_at timestamptz NOT NULL,
    projected_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (repository_id, pull_request_id),
    CONSTRAINT pull_request_first_reviews_repository_id_positive
        CHECK (repository_id > 0),
    CONSTRAINT pull_request_first_reviews_pull_request_id_positive
        CHECK (pull_request_id > 0),
    CONSTRAINT pull_request_first_reviews_review_id_positive
        CHECK (review_id > 0),
    CONSTRAINT pull_request_first_reviews_reviewer_id_positive
        CHECK (reviewer_id > 0),
    CONSTRAINT pull_request_first_reviews_reviewer_login_nonempty
        CHECK (btrim(reviewer_login) <> '')
);

COMMENT ON TABLE serving.pull_request_first_reviews IS
    'Earliest eligible non-author review retained across close and reopen transitions.';

CREATE TABLE IF NOT EXISTS serving.open_pull_requests (
    repository_id bigint NOT NULL,
    pull_request_id bigint NOT NULL,
    pull_request_number integer NOT NULL,
    repository_full_name text NOT NULL,
    title text NOT NULL,
    author_id bigint NOT NULL,
    author_login text NOT NULL,
    is_draft boolean NOT NULL,
    opened_at timestamptz NOT NULL,
    last_source_updated_at timestamptz NOT NULL,
    first_eligible_review_at timestamptz,
    first_eligible_review_id bigint,
    first_eligible_reviewer_id bigint,
    first_eligible_reviewer_login text,
    projected_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (repository_id, pull_request_id),
    CONSTRAINT open_pull_requests_repository_number_unique
        UNIQUE (repository_id, pull_request_number),
    CONSTRAINT open_pull_requests_repository_id_positive
        CHECK (repository_id > 0),
    CONSTRAINT open_pull_requests_pull_request_id_positive
        CHECK (pull_request_id > 0),
    CONSTRAINT open_pull_requests_pull_request_number_positive
        CHECK (pull_request_number > 0),
    CONSTRAINT open_pull_requests_author_id_positive
        CHECK (author_id > 0),
    CONSTRAINT open_pull_requests_repository_full_name_nonempty
        CHECK (btrim(repository_full_name) <> ''),
    CONSTRAINT open_pull_requests_title_nonempty
        CHECK (btrim(title) <> ''),
    CONSTRAINT open_pull_requests_author_login_nonempty
        CHECK (btrim(author_login) <> ''),
    CONSTRAINT open_pull_requests_review_fields_complete
        CHECK (
            (
                first_eligible_review_at IS NULL
                AND first_eligible_review_id IS NULL
                AND first_eligible_reviewer_id IS NULL
                AND first_eligible_reviewer_login IS NULL
            )
            OR (
                first_eligible_review_at IS NOT NULL
                AND first_eligible_review_id > 0
                AND first_eligible_reviewer_id > 0
                AND btrim(first_eligible_reviewer_login) <> ''
            )
        )
);

COMMENT ON TABLE serving.open_pull_requests IS
    'Current open pull-request projection for flow monitoring and stale-PR sweeps.';
COMMENT ON COLUMN serving.open_pull_requests.first_eligible_review_at IS
    'Earliest submitted review timestamp from a reviewer other than the PR author.';

CREATE INDEX IF NOT EXISTS open_pull_requests_stale_sweep_idx
    ON serving.open_pull_requests (opened_at)
    WHERE first_eligible_review_at IS NULL AND NOT is_draft;

CREATE TABLE IF NOT EXISTS ops.alert_outbox (
    alert_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    alert_key text NOT NULL,
    alert_type text NOT NULL,
    repository_id bigint NOT NULL,
    pull_request_id bigint NOT NULL,
    pull_request_number integer NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    payload jsonb NOT NULL,
    status text NOT NULL DEFAULT 'pending',
    attempt_count integer NOT NULL DEFAULT 0,
    dispatched_at timestamptz,
    last_error text,
    CONSTRAINT alert_outbox_alert_key_unique
        UNIQUE (alert_key),
    CONSTRAINT alert_outbox_alert_key_nonempty
        CHECK (btrim(alert_key) <> ''),
    CONSTRAINT alert_outbox_type_allowed
        CHECK (alert_type IN ('stale_pull_request')),
    CONSTRAINT alert_outbox_status_allowed
        CHECK (status IN ('pending', 'sent', 'cancelled')),
    CONSTRAINT alert_outbox_repository_id_positive
        CHECK (repository_id > 0),
    CONSTRAINT alert_outbox_pull_request_id_positive
        CHECK (pull_request_id > 0),
    CONSTRAINT alert_outbox_pull_request_number_positive
        CHECK (pull_request_number > 0),
    CONSTRAINT alert_outbox_attempt_count_nonnegative
        CHECK (attempt_count >= 0),
    CONSTRAINT alert_outbox_sent_has_dispatch_time
        CHECK (status <> 'sent' OR dispatched_at IS NOT NULL)
);

COMMENT ON TABLE ops.alert_outbox IS
    'Durable, idempotent alert intents; external dispatch is a later checkpoint.';
COMMENT ON COLUMN ops.alert_outbox.alert_key IS
    'Stable effect identity; stale PR alerts use repository and pull-request IDs.';

CREATE INDEX IF NOT EXISTS alert_outbox_pending_created_idx
    ON ops.alert_outbox (created_at)
    WHERE status = 'pending';
