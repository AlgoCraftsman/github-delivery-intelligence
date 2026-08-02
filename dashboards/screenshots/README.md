# Dashboard screenshot evidence

These PNGs are generated from the real local Metabase UI after reloading the isolated
fixture table, running fixture-backed dbt freshness/build, and passing
`make dashboard-sql-check`. They are not mocked, and they are not a substitute for
the checked-in query contracts.

## Regeneration workflow

1. Run the deterministic fixture workflow in `dashboards/README.md`.
2. Run `make dashboard-up` and verify <http://localhost:3000/api/health> returns
   `{"status":"ok"}`.
3. Sign in at <http://localhost:3000> with the documented local-only demo account.
4. Open **Delivery performance** and wait until every card has real axes, legends,
   values, and table rows rather than loading skeletons.
5. Capture `delivery-performance.png` from the actual dashboard UI.
6. Open **Pull-request flow**, wait for all five cards, and capture
   `pull-request-flow.png`.
7. Open both files in an image viewer and inspect the pixels, not only the DOM or API
   response. Restore any temporary viewport override afterward.

## Visual QA checklist

For **Delivery performance**, confirm:

- the title and P50/P90 legend labels are legible;
- plotted P50/P90 points come from the fixture's linked measured and configured-proxy
  changes;
- `configured_proxy`, `measured`, and `unavailable` are visible;
- coverage numerator, denominator, ratio, and `exclusion_reason` are visible;
- no contributor ranking or current-time-dependent analytics appears.

For **Pull-request flow**, confirm:

- all five cards render without errors;
- the review-latency P50/P90 legend is legible;
- size labels are `XS (<=50)`, `S (51-200)`, `M (201-500)`, and `L (>500)`;
- rework cycles are ordered numerically;
- the aging table shows draft status, the fixed `2026-01-14 12:00 UTC` `as_of`, age,
  bucket, review state, and repository WIP;
- no contributor ranking or current-time-dependent analytics appears.

The passwords documented for this local demonstration are not production-safe.
