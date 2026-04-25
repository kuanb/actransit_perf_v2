# Testing

Two tiers, both invoked via `make`.

## Unit tests — `make test`

Runs `go test ./... -race -v`. Pure Go, no network, no GCP calls. Safe to
run any time.

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
  `staleThreshold` (20 min), and the `maxProbesPerTrip` (20) cap.

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
