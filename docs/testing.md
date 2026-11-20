# Testing

Two tiers, both invoked via `make`. Also: a pre-commit hook and GitHub
Actions CI keep regressions from sneaking in.

## Unit tests — `make test`

Runs `go test ./... -race -v` plus the pure Python tests under
`services/data_mcp/tests` and `services/agent/tests`. No network or GCP calls
are made. Safe to run any time.

What's covered:

- **`cmd/scraper/geo_test.go`** — `haversineMeters`,
  `projectPointOntoSegment`, `projectLatLonOntoShape`. Cases include
  same-point (zero distance), known short distances, NYC↔LA (~3,944 km),
  antimeridian crossing, segment endpoint clamping, zero-length segments,
  perpendicular-offset projections, and degenerate shapes.
- **`cmd/scraper/gtfs_static_test.go`** — `processGTFS` against an
  in-memory synthetic GTFS zip. Covers route grouping, trip-to-shape-to-
  stop wiring, monotonically-increasing `dist_along_route_m`, projection
  consistency for trips that share a shape and stop, and a regression test
  for a UTF-8 BOM at the start of `stops.txt` (which broke parsing
  previously). Plus a table test for `sanitizeRouteID`.
- **`cmd/scraper/track_test.go`** — `updateInFlightState`, the pure
  state-machine half of `trackPerformance`. Covers new vehicle, continuing
  trip, trip change for the same vehicle, stale pruning past
  `staleThreshold` (20 min), and the `maxProbesPerTrip` (20) cap. Also
  covers `projectInFlightProbes` (fills `dist_along_route_m` and
  `nearest_stop_seq` per probe; skips trips with unknown shapes; doesn't
  overwrite already-projected probes) and `detectStopArrivals` (probe
  before/at/past a stop, two-probe linear interpolation across a stop,
  preservation of already-recorded arrivals, single probe past multiple
  stops, unknown-route safety).
- **`cmd/scraper/gtfs_cache_test.go`** — `nearestStopSeq` with
  before-first / on-stop / between / past-last cases plus empty input.
- **`cmd/scraper/bq_test.go`** — `parseServiceDate` (GTFS YYYYMMDD →
  civil.Date), `parseScheduledArrival` (GTFS-static HH:MM:SS combined
  with service_date in America/Los_Angeles, including post-midnight
  hours like 25:30), and `tripToRows` (happy path with delay + leg
  computations, unknown route → 0 obs but probes still emit, no
  StopArrivals → NULL actual fields, malformed service_date doesn't
  short-circuit row emission).
- **`cmd/scraper/track_test.go`** also covers `detectCompletedTrips` —
  trip with last-stop arrival → finalized; trip without → kept;
  unknown route → preserved (we can't tell which stop is last); nil
  cache → no-op.
- **`services/data_mcp/tests/test_core.py`** — deterministic timeframe defaults,
  disclosure of inferred dates, count-weighted metric aggregation, route-group
  classification, and rejection of cross-group comparisons.
- **`services/agent/tests/test_policy.py`** — route-scope enforcement, route-page
  week defaults, regular-service ranking defaults, category-sensitive eval
  tagging, eval-case integrity, and structured response caps for maps, charts, and tables.
- **`services/agent/tests/test_recorder.py`** — private conversation-record schema,
  UTC date/hour object layout, complete question/response retention, browser
  session hashing, and the separate minimal public-turn schema with text
  redaction and operational-field exclusion.

## Live route-category model eval

`services/agent/evals/route_category_cases.json` contains synthetic prompts for
regular, 600-series school, and 800-series All Nighter rankings plus a mixed
comparison refusal. Run them manually against a deployed agent:

```sh
make eval-analysis-route-groups ANALYSIS_API_URL=https://actransit-analysis-agent-komsnvluna-uw.a.run.app
make eval-analysis-conversations ANALYSIS_API_URL=https://actransit-analysis-agent-komsnvluna-uw.a.run.app
```

The route-category suite consumes five accepted questions. The conversation
suite consumes five more across a Route 1T chart thread and a Route 51A spatial
thread that requires stop and section maps. Both wait through the configured
cooldown. They are not CI checks because they use Vertex AI, the live public daily
allowance, and production data. Inspect recent structured traces with
`make analysis-eval-logs`; logs omit raw prompts and retain only feature tags,
bounded tool arguments, request IDs, and policy outcomes.

Raw accepted questions and structured responses are intentionally kept out of
Cloud Logging and written to the private conversation archive instead. Inspect
an archive partition with `make analysis-chat-logs DATE=YYYY-MM-DD HOUR=HH`.
Successful future turns also produce a redacted public record for the static
conversation-history page; private records created before the public notice are
not backfilled.

## Managed-model comparison eval

`services/agent/evals/model_comparison_cases.json` is a deterministic suite built
from the production policy and representative historical questions. It checks
tool selection, required arguments, scope and service-group policy, expected
map/chart artifacts, answer terminology, latency, token use, and estimated cost.
The evaluator derives the tool descriptions and JSON schemas directly from
`services/data_mcp/server.py`, then supplies fixture results instead of querying
BigQuery. It calls the production `AnalysisAgent` and provider adapters, so the
prompt, tool loop, provider request shape, and token accounting are the same code
used by Cloud Run. This isolates model and interface behavior, avoids the public
chatbot allowance, and makes MCP signature or description changes part of the
next run.

Run the default comparison across Gemini 3.5 Flash-Lite, Gemma 4, gpt-oss 120B,
DeepSeek V4.1 Flash, Llama 4 Scout, Mistral Small 3.1, and Grok 4.7 with
Application Default Credentials that can call Vertex AI:

```sh
make eval-analysis-models EVAL_REPEATS=3
```

Override `EVAL_MODELS` with comma-separated `gemini:MODEL`,
`open:PUBLISHER/MODEL`, or `mistral:MODEL` values. A non-global endpoint can use
`@LOCATION`, for example `mistral:mistral-small-2503@us-central1`. Detailed
per-turn records, a JSON summary, and a comparison CSV are written under
`output/model-eval/`. Prices are estimates pinned in the evaluator with an
as-of date; update them when Vertex pricing changes. During case development,
pass `--case CASE_ID` directly to the Python runner to run only selected cases.
The evaluator uses a 90-second request timeout by default so slower reasoning
models can complete without changing the production service's 30-second model
timeout. It retries one failed provider request before skipping the rest of that
model's paid calls, which tolerates a transient backend response without
repeatedly calling an unavailable or unentitled model.

`.github/workflows/model-eval.yml` exposes the same comparison through a manual
GitHub Actions `workflow_dispatch`. It does not run on pushes or pull requests.
Terraform creates a least-privilege evaluator service account and a GitHub OIDC
Workload Identity provider, so the workflow does not need a service-account key.
Open-model access must already be enabled once by a project administrator; the
workflow identity can invoke enabled models but cannot procure new entitlements.
After applying Terraform, set these repository Actions variables from the
matching outputs:

```sh
cd infra
terraform output -raw model_eval_workload_identity_provider
terraform output -raw model_eval_service_account
```

Name the variables `GCP_WORKLOAD_IDENTITY_PROVIDER` and
`GCP_MODEL_EVAL_SERVICE_ACCOUNT`. The workflow shows the comparison table in
the run summary, uploads the detailed files for 30 days, and fails when a model
has API errors or misses the requested overall or critical-case thresholds.
After a provider API error or 30-second model-call timeout, the runner records
that failure and skips further paid requests to the same model for that run.

The production adapter is selected independently of the eval list. The default
deployment uses:

```text
ANALYSIS_MODEL_PROVIDER=vertex_open
ANALYSIS_MODEL=google/gemma-4-26b-a4b-it-maas
```

To roll back without changing the MCP, prompt, policy, or browser interface,
use:

```text
ANALYSIS_MODEL_PROVIDER=gemini
ANALYSIS_MODEL=gemini-3.5-flash-lite
```

## Smoke test — `make smoke`

Bash + `curl` + `gsutil` against the deployed Cloud Run service. **Run
this only after a deploy.** It exercises the live service end-to-end and
performs side effects (writes `latest.json`, `route_stops.json`, and
`state.json` in GCS). Requires `gcloud auth login`.

What it does:

1. `GET /` — health check, expects `200`.
2. `POST /scrape` then `gsutil stat gs://.../latest.json`.
3. `POST /refresh-stops` then `gsutil stat gs://.../route_stops.json`.
4. `POST /track-performance` then `gsutil stat gs://.../state.json`.
5. `POST /refresh-gtfs` is **off by default** (it downloads ~14 MB).
   Pass `make smoke TAG=full` to include it.

The script fails fast on the first non-`200` response or missing object.

## What is intentionally NOT unit-tested

The boundary code that interacts with:

- Google Cloud Storage (`storage.NewClient`, `Bucket().Object()...`)
- Secret Manager (`secretmanager.NewClient`, `AccessSecretVersion`)
- Cloud Monitoring (`metricsClient.CreateTimeSeries`)

Mocking these via interfaces would force production code to carry an
abstraction it doesn't otherwise need, in service of tests rather than
the runtime. We get coverage on these by running `make smoke` against
the real services after each deploy. The trade-off: there's a window
between code change and smoke run where boundary regressions can slip
in. For a pet project deployed by one person, that's acceptable.

## Adding a new test

Test files live next to the code they exercise: `cmd/scraper/foo.go`
gets `cmd/scraper/foo_test.go`, package `main`. Use the existing test
files as templates:

- Pure math/logic test → see `geo_test.go`.
- Test that needs a GTFS zip → reuse `buildSyntheticGTFSZip` in
  `gtfs_static_test.go`.
- Test that exercises trip-lifecycle logic → use `mkSnapshot` from
  `track_test.go` and call `updateInFlightState` directly.

If a new test would require a GCP client to be mocked, write the test as
an additional smoke check in the `Makefile` instead of pulling an
interface seam into production code.

## Pre-commit hook

A version-controlled `pre-commit` hook lives at `.githooks/pre-commit`
and runs `go vet ./...` + `go test ./...` before every commit. Install
once per clone:

```sh
make hooks-install
```

It symlinks `.git/hooks/pre-commit` → `.githooks/pre-commit`, so future
hook updates land automatically when you `git pull`. Skip ad hoc with
`git commit --no-verify` (use sparingly).

## GitHub Actions CI

.github/workflows/test.yml` runs `go vet`, `go test ./... -race`, the Python
analysis tests, and Python bytecode compilation on every push to `main` and on
every pull request. No GCP credentials are needed because unit tests are pure
(per the boundary policy above) — the runner is a vanilla `ubuntu-latest`.

The separate `model-eval.yml` workflow is manual and uses real Vertex calls
against deterministic tools. It is intended for deliberate model, prompt,
interface, or MCP comparisons rather than routine CI.

`make smoke` is **not** wired into CI by design. It would require either
a service account JSON key or a Workload Identity Federation setup, plus
ongoing concern about CI invocations causing real GCS writes. Run smoke
manually after each `make release`.
