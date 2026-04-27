PROJECT_ID := transit-203605
REGION     := us-west1
REPO       := actransit-scraper
SERVICE    := actransit-scraper
IMAGE      := $(REGION)-docker.pkg.dev/$(PROJECT_ID)/$(REPO)/scraper

# Auto-derive the image tag from git: short SHA, plus "-dirty" when the
# working tree has uncommitted changes to tracked files. This makes every
# commit a unique image tag (no chance of re-tagging an old SHA and Cloud
# Run silently keeping the prior digest) and gates accidental shipping of
# uncommitted code (release prompts before building a -dirty tag).
GIT_SHA  := $(shell git rev-parse --short HEAD 2>/dev/null)
DIRTY    := $(shell git diff-index --quiet HEAD -- 2>/dev/null || echo "-dirty")
AUTO_TAG := $(GIT_SHA)$(DIRTY)

.PHONY: tf-init tf-plan tf-apply tf-fmt build deploy release logs invoke run-local test smoke hooks-install backfill

tf-init:
	cd infra && terraform init

tf-plan:
	cd infra && terraform plan

tf-apply:
	cd infra && terraform apply

tf-fmt:
	cd infra && terraform fmt -recursive

# build defaults TAG to AUTO_TAG (current git short SHA, +-dirty if uncommitted).
# Override with `make build TAG=...` for a friendly name (e.g. hotfix1).
build:
	@T=$${TAG:-$(AUTO_TAG)}; \
	test -n "$$T" || (echo "ERROR: couldn't determine tag (no TAG= and git unreadable)" && exit 1); \
	echo "==> building $(IMAGE):$$T"; \
	gcloud builds submit --tag $(IMAGE):$$T

# deploy defaults TAG to whatever's currently running on Cloud Run. So a
# bare `make deploy` applies infra changes against the live image — it
# physically can't downgrade or accidentally pin to v1. Pass TAG= to
# override (e.g. when shipping a build-then-deploy in two separate steps).
deploy:
	@if [ -n "$$TAG" ]; then T="$$TAG"; else \
	  IMG=$$(gcloud run services describe $(SERVICE) --region=$(REGION) \
	    --format='value(spec.template.spec.containers[].image)' 2>/dev/null); \
	  case "$$IMG" in *@*) echo "ERROR: live image is digest-pinned (no tag). Pass TAG= explicitly." && exit 1;; esac; \
	  T="$${IMG##*:}"; fi; \
	test -n "$$T" || (echo "ERROR: no TAG= and couldn't read currently-deployed tag" && exit 1); \
	gcloud artifacts docker tags list $(IMAGE) --format='value(TAG)' | grep -qx "$$T" \
	  || (echo "ERROR: tag '$$T' not in Artifact Registry — run 'make build TAG=$$T' first" && exit 1); \
	echo "==> deploying $$T"; \
	cd infra && terraform apply -var "image_tag=$$T"

# release builds + deploys at the auto-derived tag. If the working tree is
# dirty, prompts before proceeding so you don't accidentally ship uncommitted
# code under a -dirty suffix.
release:
	@T=$${TAG:-$(AUTO_TAG)}; \
	case "$$T" in \
	  *-dirty) \
	    echo "WARN: working tree is dirty; tag will be '$$T' (uncommitted code)"; \
	    read -p "Proceed? [y/N] " yn; [ "$$yn" = "y" ] || (echo "aborted." && exit 1);; \
	esac; \
	$(MAKE) build TAG=$$T && $(MAKE) deploy TAG=$$T

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
