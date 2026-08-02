\set ON_ERROR_STOP on

BEGIN;

CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.github_events_fixture (
    event_id uuid PRIMARY KEY,
    source text NOT NULL,
    source_record_key text NOT NULL,
    delivery_id text,
    event_name text NOT NULL,
    action text NOT NULL,
    repository_id bigint NOT NULL,
    installation_id bigint NOT NULL,
    occurred_at timestamptz,
    received_at timestamptz,
    ingested_at timestamptz NOT NULL,
    payload jsonb NOT NULL,
    kafka_partition integer,
    kafka_offset bigint,
    UNIQUE (source, source_record_key)
);

TRUNCATE TABLE raw.github_events_fixture;

INSERT INTO raw.github_events_fixture (
    event_id,
    source,
    source_record_key,
    delivery_id,
    event_name,
    action,
    repository_id,
    installation_id,
    occurred_at,
    received_at,
    ingested_at,
    payload,
    kafka_partition,
    kafka_offset
)
VALUES
(
    '00000000-0000-0000-0000-000000000001',
    'webhook',
    'fixture-pr-webhook',
    'fixture-pr-webhook',
    'pull_request',
    'opened',
    20001,
    10001,
    '2026-01-10T10:00:00Z',
    '2026-01-10T10:00:01Z',
    statement_timestamp() - interval '2 minutes',
    $json${
      "repository": {"id": 20001, "full_name": "example-org/delivery-demo"},
      "pull_request": {
        "id": 30001,
        "node_id": "PR_NODE_17",
        "number": 17,
        "state": "open",
        "title": "Normalize delivery fixtures",
        "body": "Synthetic webhook PR",
        "draft": false,
        "user": {"id": 40001, "node_id": "USER_NODE_1", "login": "example-author"},
        "created_at": "2026-01-10T10:00:00Z",
        "updated_at": "2026-01-10T10:00:00Z",
        "base": {"ref": "main"},
        "head": {"ref": "feature/fixtures"},
        "additions": 12,
        "deletions": 3,
        "changed_files": 2
      }
    }$json$::jsonb,
    0,
    1
),
(
    '00000000-0000-0000-0000-000000000002',
    'backfill',
    'fixture-pr-backfill',
    NULL,
    'pull_request',
    'merged',
    20001,
    10001,
    '2026-01-10T10:00:00Z',
    NULL,
    statement_timestamp() - interval '2 minutes',
    $json${
      "resource": "pull_request",
      "repository": {"id": 20001, "full_name": "example-org/delivery-demo"},
      "pull_request": {
        "id": "PR_NODE_17",
        "number": 17,
        "state": "MERGED",
        "title": "Normalize delivery fixtures",
        "body": "Synthetic GraphQL PR",
        "isDraft": false,
        "author": {"id": "USER_NODE_1", "login": "example-author"},
        "createdAt": "2026-01-10T10:00:00Z",
        "updatedAt": "2026-01-12T12:00:00Z",
        "closedAt": "2026-01-12T12:00:00Z",
        "mergedAt": "2026-01-12T12:00:00Z",
        "baseRefName": "main",
        "headRefName": "feature/fixtures",
        "mergeCommit": {"id": "COMMIT_NODE_MERGE", "oid": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
        "additions": 12,
        "deletions": 3,
        "changedFiles": 2
      }
    }$json$::jsonb,
    NULL,
    NULL
),
(
    '00000000-0000-0000-0000-000000000003',
    'webhook',
    'fixture-review-webhook',
    'fixture-review-webhook',
    'pull_request_review',
    'submitted',
    20001,
    10001,
    '2026-01-11T09:00:00Z',
    '2026-01-11T09:00:01Z',
    statement_timestamp() - interval '2 minutes',
    $json${
      "repository": {"id": 20001, "full_name": "example-org/delivery-demo"},
      "pull_request": {"id": 30001, "node_id": "PR_NODE_17", "number": 17},
      "review": {
        "id": 50001,
        "node_id": "REVIEW_NODE_1",
        "state": "approved",
        "body": "Synthetic approval",
        "commit_id": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "submitted_at": "2026-01-11T09:00:00Z",
        "user": {"id": 40002, "node_id": "USER_NODE_2", "login": "example-reviewer"}
      }
    }$json$::jsonb,
    1,
    2
),
(
    '00000000-0000-0000-0000-000000000004',
    'backfill',
    'fixture-review-backfill',
    NULL,
    'pull_request_review',
    'approved',
    20001,
    10001,
    '2026-01-11T09:00:00Z',
    NULL,
    statement_timestamp() - interval '2 minutes',
    $json${
      "resource": "pull_request_review",
      "repository": {"id": 20001, "full_name": "example-org/delivery-demo"},
      "pull_request_id": "PR_NODE_17",
      "pull_request_review": {
        "id": "REVIEW_NODE_1",
        "fullDatabaseId": "50001",
        "state": "APPROVED",
        "body": "Synthetic approval",
        "submittedAt": "2026-01-11T09:00:00Z",
        "updatedAt": "2026-01-11T09:00:00Z",
        "author": {"id": "USER_NODE_2", "login": "example-reviewer"},
        "commit": {"id": "COMMIT_NODE_1", "oid": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}
      }
    }$json$::jsonb,
    NULL,
    NULL
),
(
    '00000000-0000-0000-0000-000000000005',
    'backfill',
    'fixture-pr-commit-backfill',
    NULL,
    'pull_request_commit',
    'associated',
    20001,
    10001,
    '2026-01-10T11:00:00Z',
    NULL,
    statement_timestamp() - interval '2 minutes',
    $json${
      "resource": "pull_request_commit",
      "repository": {"id": 20001, "full_name": "example-org/delivery-demo"},
      "pull_request_id": "PR_NODE_17",
      "pull_request_commit": {
        "id": "PR_COMMIT_NODE_1",
        "commit": {
          "id": "COMMIT_NODE_1",
          "oid": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
          "authoredDate": "2026-01-10T10:55:00Z",
          "committedDate": "2026-01-10T11:00:00Z",
          "message": "Add deterministic dbt fixtures",
          "additions": 12,
          "deletions": 3,
          "changedFilesIfAvailable": 2,
          "author": {
            "name": "Example Author",
            "email": "author@example.invalid",
            "user": {"id": "USER_NODE_1", "login": "example-author"}
          }
        }
      }
    }$json$::jsonb,
    NULL,
    NULL
),
(
    '00000000-0000-0000-0000-000000000006',
    'webhook',
    'fixture-workflow-webhook',
    'fixture-workflow-webhook',
    'workflow_run',
    'completed',
    20001,
    10001,
    '2026-01-12T12:30:00Z',
    '2026-01-12T12:35:01Z',
    statement_timestamp() - interval '2 minutes',
    $json${
      "repository": {"id": 20001, "full_name": "example-org/delivery-demo"},
      "workflow_run": {
        "id": 60001,
        "run_attempt": 1,
        "workflow_id": 61001,
        "name": "Synthetic CI",
        "event": "pull_request",
        "status": "completed",
        "conclusion": "success",
        "head_branch": "feature/fixtures",
        "head_sha": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "created_at": "2026-01-12T12:30:00Z",
        "run_started_at": "2026-01-12T12:31:00Z",
        "updated_at": "2026-01-12T12:35:00Z"
      }
    }$json$::jsonb,
    2,
    3
),
(
    '00000000-0000-0000-0000-000000000007',
    'backfill',
    'fixture-workflow-backfill',
    NULL,
    'workflow_run',
    'completed',
    20001,
    10001,
    '2026-01-12T12:30:00Z',
    NULL,
    statement_timestamp() - interval '2 minutes',
    $json${
      "resource": "workflow_run",
      "repository": {"id": 20001, "full_name": "example-org/delivery-demo"},
      "workflow_run": {
        "id": 60001,
        "run_attempt": 1,
        "workflow_id": 61001,
        "name": "Synthetic CI",
        "event": "pull_request",
        "status": "completed",
        "conclusion": "success",
        "head_branch": "feature/fixtures",
        "head_sha": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "created_at": "2026-01-12T12:30:00Z",
        "run_started_at": "2026-01-12T12:31:00Z",
        "updated_at": "2026-01-12T12:35:00Z"
      }
    }$json$::jsonb,
    NULL,
    NULL
),
(
    '00000000-0000-0000-0000-000000000008',
    'webhook',
    'fixture-deployment-webhook',
    'fixture-deployment-webhook',
    'deployment',
    'created',
    20001,
    10001,
    '2026-01-13T14:00:00Z',
    '2026-01-13T14:00:01Z',
    statement_timestamp() - interval '2 minutes',
    $json${
      "repository": {"id": 20001, "full_name": "example-org/delivery-demo"},
      "deployment": {
        "id": 70001,
        "environment": "production",
        "sha": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "ref": "main",
        "task": "deploy",
        "transient_environment": false,
        "production_environment": true,
        "created_at": "2026-01-13T14:00:00Z",
        "updated_at": "2026-01-13T14:00:00Z"
      }
    }$json$::jsonb,
    0,
    4
),
(
    '00000000-0000-0000-0000-000000000009',
    'backfill',
    'fixture-deployment-backfill',
    NULL,
    'deployment',
    'created',
    20001,
    10001,
    '2026-01-13T14:00:00Z',
    NULL,
    statement_timestamp() - interval '2 minutes',
    $json${
      "resource": "deployment",
      "repository": {"id": 20001, "full_name": "example-org/delivery-demo"},
      "deployment": {
        "id": 70001,
        "environment": "production",
        "sha": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "ref": "main",
        "task": "deploy",
        "transient_environment": false,
        "production_environment": true,
        "created_at": "2026-01-13T14:00:00Z",
        "updated_at": "2026-01-13T14:00:00Z"
      }
    }$json$::jsonb,
    NULL,
    NULL
),
(
    '00000000-0000-0000-0000-000000000010',
    'webhook',
    'fixture-deployment-status-webhook',
    'fixture-deployment-status-webhook',
    'deployment_status',
    'created',
    20001,
    10001,
    '2026-01-13T14:05:00Z',
    '2026-01-13T14:05:01Z',
    statement_timestamp() - interval '2 minutes',
    $json${
      "repository": {"id": 20001, "full_name": "example-org/delivery-demo"},
      "deployment": {"id": 70001, "environment": "production"},
      "deployment_status": {
        "id": 80001,
        "state": "success",
        "environment": "production",
        "description": "Synthetic deployment succeeded",
        "environment_url": "https://example.invalid/environment",
        "log_url": "https://example.invalid/log",
        "creator": {"id": 40003, "login": "example-deployer"},
        "created_at": "2026-01-13T14:05:00Z",
        "updated_at": "2026-01-13T14:05:00Z"
      }
    }$json$::jsonb,
    1,
    5
),
(
    '00000000-0000-0000-0000-000000000011',
    'backfill',
    'fixture-deployment-status-backfill',
    NULL,
    'deployment_status',
    'success',
    20001,
    10001,
    '2026-01-13T14:05:00Z',
    NULL,
    statement_timestamp() - interval '2 minutes',
    $json${
      "resource": "deployment_status",
      "repository": {"id": 20001, "full_name": "example-org/delivery-demo"},
      "deployment_id": 70001,
      "deployment_status": {
        "id": 80001,
        "state": "success",
        "environment": "production",
        "description": "Synthetic deployment succeeded",
        "environment_url": "https://example.invalid/environment",
        "log_url": "https://example.invalid/log",
        "creator": {"id": 40003, "login": "example-deployer"},
        "created_at": "2026-01-13T14:05:00Z",
        "updated_at": "2026-01-13T14:05:00Z"
      }
    }$json$::jsonb,
    NULL,
    NULL
),
(
    '00000000-0000-0000-0000-000000000012',
    'backfill',
    'fixture-unmatched-pr-backfill',
    NULL,
    'pull_request',
    'merged',
    20001,
    10001,
    '2026-01-10T11:00:00Z',
    NULL,
    statement_timestamp() - interval '2 minutes',
    $json${
      "resource": "pull_request",
      "repository": {"id": 20001, "full_name": "example-org/delivery-demo"},
      "pull_request": {
        "id": "PR_NODE_18",
        "number": 18,
        "state": "MERGED",
        "title": "Leave a change intentionally unmatched",
        "isDraft": false,
        "author": {"id": "USER_NODE_3", "login": "example-author-two"},
        "createdAt": "2026-01-10T11:00:00Z",
        "updatedAt": "2026-01-12T13:00:00Z",
        "closedAt": "2026-01-12T13:00:00Z",
        "mergedAt": "2026-01-12T13:00:00Z",
        "baseRefName": "main",
        "headRefName": "feature/unmatched",
        "mergeCommit": {"id": "COMMIT_NODE_UNMATCHED", "oid": "cccccccccccccccccccccccccccccccccccccccc"},
        "additions": 5,
        "deletions": 1,
        "changedFiles": 1
      }
    }$json$::jsonb,
    NULL,
    NULL
),
(
    '00000000-0000-0000-0000-000000000013',
    'backfill',
    'fixture-unconfigured-pr-backfill',
    NULL,
    'pull_request',
    'merged',
    20002,
    10002,
    '2026-01-10T12:00:00Z',
    NULL,
    statement_timestamp() - interval '2 minutes',
    $json${
      "resource": "pull_request",
      "repository": {"id": 20002, "full_name": "example-org/unconfigured-demo"},
      "pull_request": {
        "id": "PR_NODE_UNCONFIGURED_1",
        "number": 1,
        "state": "MERGED",
        "title": "Prove missing configuration remains unavailable",
        "isDraft": false,
        "author": {"id": "USER_NODE_4", "login": "unconfigured-author"},
        "createdAt": "2026-01-10T12:00:00Z",
        "updatedAt": "2026-01-12T10:00:00Z",
        "closedAt": "2026-01-12T10:00:00Z",
        "mergedAt": "2026-01-12T10:00:00Z",
        "baseRefName": "main",
        "headRefName": "feature/unconfigured",
        "mergeCommit": {"id": "COMMIT_NODE_UNCONFIGURED", "oid": "dddddddddddddddddddddddddddddddddddddddd"},
        "additions": 8,
        "deletions": 2,
        "changedFiles": 2
      }
    }$json$::jsonb,
    NULL,
    NULL
),
(
    '00000000-0000-0000-0000-000000000014',
    'backfill',
    'fixture-unconfigured-deployment-backfill',
    NULL,
    'deployment',
    'created',
    20002,
    10002,
    '2026-01-13T15:00:00Z',
    NULL,
    statement_timestamp() - interval '2 minutes',
    $json${
      "resource": "deployment",
      "repository": {"id": 20002, "full_name": "example-org/unconfigured-demo"},
      "deployment": {
        "id": 71001,
        "environment": "production",
        "sha": "dddddddddddddddddddddddddddddddddddddddd",
        "ref": "main",
        "task": "deploy",
        "transient_environment": false,
        "production_environment": true,
        "created_at": "2026-01-13T15:00:00Z",
        "updated_at": "2026-01-13T15:00:00Z"
      }
    }$json$::jsonb,
    NULL,
    NULL
),
(
    '00000000-0000-0000-0000-000000000015',
    'backfill',
    'fixture-unconfigured-deployment-status-backfill',
    NULL,
    'deployment_status',
    'success',
    20002,
    10002,
    '2026-01-13T15:10:00Z',
    NULL,
    statement_timestamp() - interval '2 minutes',
    $json${
      "resource": "deployment_status",
      "repository": {"id": 20002, "full_name": "example-org/unconfigured-demo"},
      "deployment_id": 71001,
      "deployment_status": {
        "id": 81001,
        "state": "success",
        "environment": "production",
        "description": "Synthetic unconfigured deployment succeeded",
        "creator": {"id": 40004, "login": "unconfigured-deployer"},
        "created_at": "2026-01-13T15:10:00Z",
        "updated_at": "2026-01-13T15:10:00Z"
      }
    }$json$::jsonb,
    NULL,
    NULL
),
(
    '00000000-0000-0000-0000-000000000016',
    'backfill',
    'fixture-workflow-proxy-pr-backfill',
    NULL,
    'pull_request',
    'merged',
    20003,
    10003,
    '2026-01-10T13:00:00Z',
    NULL,
    statement_timestamp() - interval '2 minutes',
    $json${
      "resource": "pull_request",
      "repository": {"id": 20003, "full_name": "example-org/workflow-proxy-demo"},
      "pull_request": {
        "id": "PR_NODE_WORKFLOW_PROXY_1",
        "number": 1,
        "state": "MERGED",
        "title": "Prove configured workflow proxy evidence",
        "isDraft": false,
        "author": {"id": "USER_NODE_5", "login": "workflow-author"},
        "createdAt": "2026-01-10T13:00:00Z",
        "updatedAt": "2026-01-12T11:00:00Z",
        "closedAt": "2026-01-12T11:00:00Z",
        "mergedAt": "2026-01-12T11:00:00Z",
        "baseRefName": "main",
        "headRefName": "feature/workflow-proxy",
        "mergeCommit": {"id": "COMMIT_NODE_WORKFLOW_PROXY", "oid": "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"},
        "additions": 9,
        "deletions": 3,
        "changedFiles": 2
      }
    }$json$::jsonb,
    NULL,
    NULL
),
(
    '00000000-0000-0000-0000-000000000017',
    'backfill',
    'fixture-production-workflow-backfill',
    NULL,
    'workflow_run',
    'completed',
    20003,
    10003,
    '2026-01-13T16:20:00Z',
    NULL,
    statement_timestamp() - interval '2 minutes',
    $json${
      "resource": "workflow_run",
      "repository": {"id": 20003, "full_name": "example-org/workflow-proxy-demo"},
      "workflow_run": {
        "id": 62001,
        "run_attempt": 1,
        "workflow_id": 63001,
        "name": "Release production",
        "event": "push",
        "status": "completed",
        "conclusion": "success",
        "head_branch": "main",
        "head_sha": "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
        "created_at": "2026-01-13T16:00:00Z",
        "run_started_at": "2026-01-13T16:01:00Z",
        "updated_at": "2026-01-13T16:20:00Z"
      }
    }$json$::jsonb,
    NULL,
    NULL
),
(
    '00000000-0000-0000-0000-000000000018',
    'backfill',
    'fixture-pr-flow-101',
    NULL,
    'pull_request',
    'merged',
    20004,
    10004,
    '2026-01-10T12:00:00Z',
    NULL,
    statement_timestamp() - interval '2 minutes',
    $json${
      "resource": "pull_request",
      "repository": {"id": 20004, "full_name": "example-org/pr-flow-demo"},
      "pull_request": {
        "id": "PR_NODE_FLOW_101", "number": 101, "state": "MERGED",
        "title": "Document the fixture contract", "isDraft": false,
        "author": {"id": "USER_NODE_FLOW_AUTHOR", "login": "flow-author"},
        "createdAt": "2026-01-10T08:00:00Z", "updatedAt": "2026-01-10T12:00:00Z",
        "closedAt": "2026-01-10T12:00:00Z", "mergedAt": "2026-01-10T12:00:00Z",
        "baseRefName": "main", "headRefName": "docs/fixture-contract",
        "mergeCommit": {"id": "COMMIT_FLOW_101", "oid": "1010101010101010101010101010101010101010"},
        "additions": 10, "deletions": 2, "changedFiles": 1
      }
    }$json$::jsonb,
    NULL,
    NULL
),
(
    '00000000-0000-0000-0000-000000000019',
    'backfill',
    'fixture-pr-flow-102',
    NULL,
    'pull_request',
    'merged',
    20004,
    10004,
    '2026-01-11T12:00:00Z',
    NULL,
    statement_timestamp() - interval '2 minutes',
    $json${
      "resource": "pull_request",
      "repository": {"id": 20004, "full_name": "example-org/pr-flow-demo"},
      "pull_request": {
        "id": "PR_NODE_FLOW_102", "number": 102, "state": "MERGED",
        "title": "Add a bounded retry policy", "isDraft": false,
        "author": {"id": "USER_NODE_FLOW_AUTHOR", "login": "flow-author"},
        "createdAt": "2026-01-10T09:00:00Z", "updatedAt": "2026-01-11T12:00:00Z",
        "closedAt": "2026-01-11T12:00:00Z", "mergedAt": "2026-01-11T12:00:00Z",
        "baseRefName": "main", "headRefName": "feature/retry-policy",
        "mergeCommit": {"id": "COMMIT_FLOW_102", "oid": "1020102010201020102010201020102010201020"},
        "additions": 80, "deletions": 20, "changedFiles": 4
      }
    }$json$::jsonb,
    NULL,
    NULL
),
(
    '00000000-0000-0000-0000-000000000020',
    'backfill',
    'fixture-pr-flow-103',
    NULL,
    'pull_request',
    'merged',
    20004,
    10004,
    '2026-01-13T05:00:00Z',
    NULL,
    statement_timestamp() - interval '2 minutes',
    $json${
      "resource": "pull_request",
      "repository": {"id": 20004, "full_name": "example-org/pr-flow-demo"},
      "pull_request": {
        "id": "PR_NODE_FLOW_103", "number": 103, "state": "MERGED",
        "title": "Refactor projection checkpoints", "isDraft": false,
        "author": {"id": "USER_NODE_FLOW_AUTHOR", "login": "flow-author"},
        "createdAt": "2026-01-10T05:00:00Z", "updatedAt": "2026-01-13T05:00:00Z",
        "closedAt": "2026-01-13T05:00:00Z", "mergedAt": "2026-01-13T05:00:00Z",
        "baseRefName": "main", "headRefName": "feature/checkpoint-refactor",
        "mergeCommit": {"id": "COMMIT_FLOW_103", "oid": "1030103010301030103010301030103010301030"},
        "additions": 280, "deletions": 70, "changedFiles": 10
      }
    }$json$::jsonb,
    NULL,
    NULL
),
(
    '00000000-0000-0000-0000-000000000021',
    'backfill',
    'fixture-pr-flow-104',
    NULL,
    'pull_request',
    'merged',
    20004,
    10004,
    '2026-01-13T12:00:00Z',
    NULL,
    statement_timestamp() - interval '2 minutes',
    $json${
      "resource": "pull_request",
      "repository": {"id": 20004, "full_name": "example-org/pr-flow-demo"},
      "pull_request": {
        "id": "PR_NODE_FLOW_104", "number": 104, "state": "MERGED",
        "title": "Split the ingestion service", "isDraft": false,
        "author": {"id": "USER_NODE_FLOW_AUTHOR", "login": "flow-author"},
        "createdAt": "2026-01-10T00:00:00Z", "updatedAt": "2026-01-13T12:00:00Z",
        "closedAt": "2026-01-13T12:00:00Z", "mergedAt": "2026-01-13T12:00:00Z",
        "baseRefName": "main", "headRefName": "feature/ingestion-split",
        "mergeCommit": {"id": "COMMIT_FLOW_104", "oid": "1040104010401040104010401040104010401040"},
        "additions": 700, "deletions": 200, "changedFiles": 20
      }
    }$json$::jsonb,
    NULL,
    NULL
),
(
    '00000000-0000-0000-0000-000000000022',
    'backfill',
    'fixture-pr-flow-105',
    NULL,
    'pull_request',
    'opened',
    20004,
    10004,
    '2026-01-10T06:00:00Z',
    NULL,
    statement_timestamp() - interval '2 minutes',
    $json${
      "resource": "pull_request",
      "repository": {"id": 20004, "full_name": "example-org/pr-flow-demo"},
      "pull_request": {
        "id": "PR_NODE_FLOW_105", "number": 105, "state": "OPEN",
        "title": "Harden offset recovery", "isDraft": false,
        "author": {"id": "USER_NODE_FLOW_AUTHOR", "login": "flow-author"},
        "createdAt": "2026-01-10T06:00:00Z", "updatedAt": "2026-01-11T06:00:00Z",
        "closedAt": null, "mergedAt": null,
        "baseRefName": "main", "headRefName": "feature/offset-recovery",
        "mergeCommit": null, "additions": 140, "deletions": 30, "changedFiles": 6
      }
    }$json$::jsonb,
    NULL,
    NULL
),
(
    '00000000-0000-0000-0000-000000000023',
    'backfill',
    'fixture-pr-flow-106',
    NULL,
    'pull_request',
    'opened',
    20004,
    10004,
    '2026-01-11T12:00:00Z',
    NULL,
    statement_timestamp() - interval '2 minutes',
    $json${
      "resource": "pull_request",
      "repository": {"id": 20004, "full_name": "example-org/pr-flow-demo"},
      "pull_request": {
        "id": "PR_NODE_FLOW_106", "number": 106, "state": "OPEN",
        "title": "Expose consumer lag metrics", "isDraft": false,
        "author": {"id": "USER_NODE_FLOW_AUTHOR", "login": "flow-author"},
        "createdAt": "2026-01-11T12:00:00Z", "updatedAt": "2026-01-11T12:00:00Z",
        "closedAt": null, "mergedAt": null,
        "baseRefName": "main", "headRefName": "feature/lag-metrics",
        "mergeCommit": null, "additions": 45, "deletions": 10, "changedFiles": 3
      }
    }$json$::jsonb,
    NULL,
    NULL
),
(
    '00000000-0000-0000-0000-000000000024',
    'backfill',
    'fixture-pr-flow-107',
    NULL,
    'pull_request',
    'opened',
    20004,
    10004,
    '2026-01-12T12:00:00Z',
    NULL,
    statement_timestamp() - interval '2 minutes',
    $json${
      "resource": "pull_request",
      "repository": {"id": 20004, "full_name": "example-org/pr-flow-demo"},
      "pull_request": {
        "id": "PR_NODE_FLOW_107", "number": 107, "state": "OPEN",
        "title": "Sketch the alert routing API", "isDraft": true,
        "author": {"id": "USER_NODE_FLOW_AUTHOR", "login": "flow-author"},
        "createdAt": "2026-01-12T12:00:00Z", "updatedAt": "2026-01-12T12:00:00Z",
        "closedAt": null, "mergedAt": null,
        "baseRefName": "main", "headRefName": "draft/alert-routing",
        "mergeCommit": null, "additions": 25, "deletions": 5, "changedFiles": 2
      }
    }$json$::jsonb,
    NULL,
    NULL
),
(
    '00000000-0000-0000-0000-000000000025',
    'backfill',
    'fixture-review-flow-101-approved',
    NULL,
    'pull_request_review',
    'submitted',
    20004,
    10004,
    '2026-01-10T10:00:00Z',
    NULL,
    statement_timestamp() - interval '2 minutes',
    $json${
      "resource": "pull_request_review",
      "repository": {"id": 20004, "full_name": "example-org/pr-flow-demo"},
      "pull_request_id": "PR_NODE_FLOW_101",
      "pull_request_review": {
        "id": "REVIEW_NODE_FLOW_101_A", "state": "APPROVED",
        "author": {"id": "USER_NODE_FLOW_REVIEWER", "login": "flow-reviewer"},
        "commit": {"oid": "FLOW_101_REV_A"},
        "submittedAt": "2026-01-10T10:00:00Z", "updatedAt": "2026-01-10T10:00:00Z"
      }
    }$json$::jsonb,
    NULL,
    NULL
),
(
    '00000000-0000-0000-0000-000000000026',
    'backfill',
    'fixture-review-flow-102-changes',
    NULL,
    'pull_request_review',
    'submitted',
    20004,
    10004,
    '2026-01-10T18:00:00Z',
    NULL,
    statement_timestamp() - interval '2 minutes',
    $json${
      "resource": "pull_request_review",
      "repository": {"id": 20004, "full_name": "example-org/pr-flow-demo"},
      "pull_request_id": "PR_NODE_FLOW_102",
      "pull_request_review": {
        "id": "REVIEW_NODE_FLOW_102_A", "state": "CHANGES_REQUESTED",
        "author": {"id": "USER_NODE_FLOW_REVIEWER", "login": "flow-reviewer"},
        "commit": {"oid": "FLOW_102_REV_A"},
        "submittedAt": "2026-01-10T18:00:00Z", "updatedAt": "2026-01-10T18:00:00Z"
      }
    }$json$::jsonb,
    NULL,
    NULL
),
(
    '00000000-0000-0000-0000-000000000027',
    'backfill',
    'fixture-review-flow-102-approved',
    NULL,
    'pull_request_review',
    'submitted',
    20004,
    10004,
    '2026-01-11T06:00:00Z',
    NULL,
    statement_timestamp() - interval '2 minutes',
    $json${
      "resource": "pull_request_review",
      "repository": {"id": 20004, "full_name": "example-org/pr-flow-demo"},
      "pull_request_id": "PR_NODE_FLOW_102",
      "pull_request_review": {
        "id": "REVIEW_NODE_FLOW_102_B", "state": "APPROVED",
        "author": {"id": "USER_NODE_FLOW_REVIEWER", "login": "flow-reviewer"},
        "commit": {"oid": "FLOW_102_REV_B"},
        "submittedAt": "2026-01-11T06:00:00Z", "updatedAt": "2026-01-11T06:00:00Z"
      }
    }$json$::jsonb,
    NULL,
    NULL
),
(
    '00000000-0000-0000-0000-000000000028',
    'backfill',
    'fixture-review-flow-103-changes-a',
    NULL,
    'pull_request_review',
    'submitted',
    20004,
    10004,
    '2026-01-11T05:00:00Z',
    NULL,
    statement_timestamp() - interval '2 minutes',
    $json${
      "resource": "pull_request_review",
      "repository": {"id": 20004, "full_name": "example-org/pr-flow-demo"},
      "pull_request_id": "PR_NODE_FLOW_103",
      "pull_request_review": {
        "id": "REVIEW_NODE_FLOW_103_A", "state": "CHANGES_REQUESTED",
        "author": {"id": "USER_NODE_FLOW_REVIEWER", "login": "flow-reviewer"},
        "commit": {"oid": "FLOW_103_REV_A"},
        "submittedAt": "2026-01-11T05:00:00Z", "updatedAt": "2026-01-11T05:00:00Z"
      }
    }$json$::jsonb,
    NULL,
    NULL
),
(
    '00000000-0000-0000-0000-000000000029',
    'backfill',
    'fixture-review-flow-103-changes-b',
    NULL,
    'pull_request_review',
    'submitted',
    20004,
    10004,
    '2026-01-12T05:00:00Z',
    NULL,
    statement_timestamp() - interval '2 minutes',
    $json${
      "resource": "pull_request_review",
      "repository": {"id": 20004, "full_name": "example-org/pr-flow-demo"},
      "pull_request_id": "PR_NODE_FLOW_103",
      "pull_request_review": {
        "id": "REVIEW_NODE_FLOW_103_B", "state": "CHANGES_REQUESTED",
        "author": {"id": "USER_NODE_FLOW_REVIEWER", "login": "flow-reviewer"},
        "commit": {"oid": "FLOW_103_REV_B"},
        "submittedAt": "2026-01-12T05:00:00Z", "updatedAt": "2026-01-12T05:00:00Z"
      }
    }$json$::jsonb,
    NULL,
    NULL
),
(
    '00000000-0000-0000-0000-000000000030',
    'backfill',
    'fixture-review-flow-103-approved',
    NULL,
    'pull_request_review',
    'submitted',
    20004,
    10004,
    '2026-01-12T17:00:00Z',
    NULL,
    statement_timestamp() - interval '2 minutes',
    $json${
      "resource": "pull_request_review",
      "repository": {"id": 20004, "full_name": "example-org/pr-flow-demo"},
      "pull_request_id": "PR_NODE_FLOW_103",
      "pull_request_review": {
        "id": "REVIEW_NODE_FLOW_103_C", "state": "APPROVED",
        "author": {"id": "USER_NODE_FLOW_REVIEWER", "login": "flow-reviewer"},
        "commit": {"oid": "FLOW_103_REV_C"},
        "submittedAt": "2026-01-12T17:00:00Z", "updatedAt": "2026-01-12T17:00:00Z"
      }
    }$json$::jsonb,
    NULL,
    NULL
),
(
    '00000000-0000-0000-0000-000000000031',
    'backfill',
    'fixture-review-flow-104-approved',
    NULL,
    'pull_request_review',
    'submitted',
    20004,
    10004,
    '2026-01-12T00:00:00Z',
    NULL,
    statement_timestamp() - interval '2 minutes',
    $json${
      "resource": "pull_request_review",
      "repository": {"id": 20004, "full_name": "example-org/pr-flow-demo"},
      "pull_request_id": "PR_NODE_FLOW_104",
      "pull_request_review": {
        "id": "REVIEW_NODE_FLOW_104_A", "state": "APPROVED",
        "author": {"id": "USER_NODE_FLOW_REVIEWER", "login": "flow-reviewer"},
        "commit": {"oid": "FLOW_104_REV_A"},
        "submittedAt": "2026-01-12T00:00:00Z", "updatedAt": "2026-01-12T00:00:00Z"
      }
    }$json$::jsonb,
    NULL,
    NULL
),
(
    '00000000-0000-0000-0000-000000000032',
    'backfill',
    'fixture-review-flow-105-changes',
    NULL,
    'pull_request_review',
    'submitted',
    20004,
    10004,
    '2026-01-11T06:00:00Z',
    NULL,
    statement_timestamp() - interval '2 minutes',
    $json${
      "resource": "pull_request_review",
      "repository": {"id": 20004, "full_name": "example-org/pr-flow-demo"},
      "pull_request_id": "PR_NODE_FLOW_105",
      "pull_request_review": {
        "id": "REVIEW_NODE_FLOW_105_A", "state": "CHANGES_REQUESTED",
        "author": {"id": "USER_NODE_FLOW_REVIEWER", "login": "flow-reviewer"},
        "commit": {"oid": "FLOW_105_REV_A"},
        "submittedAt": "2026-01-11T06:00:00Z", "updatedAt": "2026-01-11T06:00:00Z"
      }
    }$json$::jsonb,
    NULL,
    NULL
);

COMMIT;
