PROJECT_ID := transit-203605
REGION     := us-west1
REPO       := actransit-scraper
IMAGE      := $(REGION)-docker.pkg.dev/$(PROJECT_ID)/$(REPO)/scraper

.PHONY: tf-init tf-plan tf-apply tf-fmt build deploy release logs invoke run-local test smoke hooks-install backfill

tf-init:
	cd infra && terraform init

tf-plan:
	cd infra && terraform plan

tf-apply:
	cd infra && terraform apply

tf-fmt:
	cd infra && terraform fmt -recursive

build:
	@test -n "$(TAG)" || (echo "usage: make build TAG=v1" && exit 1)
	gcloud builds submit --tag $(IMAGE):$(TAG)

deploy:
	@test -n "$(TAG)" || (echo "usage: make deploy TAG=v1" && exit 1)
	@gcloud artifacts docker tags list $(IMAGE) --format='value(TAG)' | grep -qx '$(TAG)' \
		|| (echo "ERROR: image tag '$(TAG)' not found in Artifact Registry. Run 'make build TAG=$(TAG)' first." && exit 1)
	cd infra && terraform apply -var "image_tag=$(TAG)"

release:
	@test -n "$(TAG)" || (echo "usage: make release TAG=v1" && exit 1)
	$(MAKE) build TAG=$(TAG)
	$(MAKE) deploy TAG=$(TAG)

logs:
	gcloud logging read 'resource.labels.service_name="actransit-scraper" AND jsonPayload.msg!=""' --limit 50 --freshness=15m \
		--format='value(timestamp,severity,jsonPayload.msg,jsonPayload.in_flight,jsonPayload.duration_ms,jsonPayload.err)'

invoke:
	curl -H "Authorization: Bearer $$(gcloud auth print-identity-token)" \
		"$$(cd infra && terraform output -raw scraper_url)/scrape"

# Replays a past day's CSVs from gs://ac-transit/maptime/ into BigQuery and
# regenerates that date's stats. Idempotent (DELETE+INSERT). FORCE=true
# overrides the today-or-future safety guard. Use with care.
backfill:
	@test -n "$(DATE)" || (echo "usage: make backfill DATE=YYYY-MM-DD [FORCE=true]" && exit 1)
	@URL="$$(cd infra && terraform output -raw scraper_url)"; \
	TOKEN="$$(gcloud auth print-identity-token)"; \
	QS="service_date=$(DATE)"; \
	if [ "$(FORCE)" = "true" ]; then QS="$$QS&force=true"; fi; \
	echo "==> POST $$URL/backfill-day?$$QS"; \
	curl --fail --max-time 1800 -X POST -H "Authorization: Bearer $$TOKEN" \
		"$$URL/backfill-day?$$QS"

run-local:
	go run ./cmd/scraper

test:
	go test ./... -race -v

# Install the version-controlled pre-commit hook into .git/hooks/.
# Idempotent — re-run after pulling new hook changes.
hooks-install:
	@mkdir -p .git/hooks
	@ln -sf ../../.githooks/pre-commit .git/hooks/pre-commit
	@chmod +x .githooks/pre-commit
	@echo "pre-commit hook installed (symlinked from .githooks/)"

# Post-deploy smoke check. Hits the live service + verifies side-effect
# artifacts in GCS. Requires a deployed service and `gcloud auth login`.
# /refresh-gtfs is skipped by default (downloads ~14 MB); pass TAG=full to
# include it.
smoke:
	@set -euo pipefail; \
	URL="$$(cd infra && terraform output -raw scraper_url)"; \
	TOKEN="$$(gcloud auth print-identity-token)"; \
	BUCKET="gs://$(PROJECT_ID)-actransit-cache"; \
	hit() { \
		echo "==> $$1 $$2"; \
		code=$$(curl -s -o /dev/null -w '%{http_code}' -X "$$1" -H "Authorization: Bearer $$TOKEN" "$$URL$$2"); \
		if [ "$$code" != "200" ]; then echo "FAIL $$1 $$2 -> $$code"; exit 1; fi; \
		echo "  OK"; \
	}; \
	check() { \
		echo "==> gsutil stat $$1"; \
		gsutil -q stat "$$1" || { echo "FAIL: $$1 not found"; exit 1; }; \
		echo "  OK"; \
	}; \
	hit GET /; \
	hit POST /scrape; \
	check "$$BUCKET/latest.json"; \
	hit POST /refresh-stops; \
	check "$$BUCKET/route_stops.json"; \
	hit POST /track-performance; \
	check "$$BUCKET/state.json"; \
	echo "==> state.json schema check (chunk 3 fields present)"; \
	gsutil cat "$$BUCKET/state.json" | python3 -c 'import json,sys; s=json.load(sys.stdin); inflt=s.get("in_flight",[]); assert isinstance(inflt,list), "in_flight not array"; assert len(inflt)>0, "no in_flight trips"; t=inflt[0]; assert "stop_arrivals" in t or "probes" in t, "trip missing stop_arrivals/probes"; print("  OK in_flight=%d probes[0]_keys=%s" % (len(inflt), sorted(t["probes"][0].keys()) if t.get("probes") else []))' || { echo "FAIL: state.json schema check"; exit 1; }; \
	echo "==> bq tables exist (chunk 4)"; \
	bq show -q $(PROJECT_ID):actransit.trip_observations >/dev/null 2>&1 || { echo "FAIL: actransit.trip_observations missing"; exit 1; }; \
	bq show -q $(PROJECT_ID):actransit.trip_probes >/dev/null 2>&1 || { echo "FAIL: actransit.trip_probes missing"; exit 1; }; \
	echo "  OK"; \
	hit POST /generate-daily-stats; \
	check "$$BUCKET/stats/latest.json"; \
	check "$$BUCKET/stats/_index.json"; \
	if [ "$(TAG)" = "full" ]; then \
		hit POST /refresh-gtfs; \
		check "$$BUCKET/gtfs/current.zip"; \
	fi; \
	echo "==> all smoke checks passed"
