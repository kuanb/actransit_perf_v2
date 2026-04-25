PROJECT_ID := transit-203605
REGION     := us-west1
REPO       := actransit-scraper
IMAGE      := $(REGION)-docker.pkg.dev/$(PROJECT_ID)/$(REPO)/scraper

.PHONY: tf-init tf-plan tf-apply tf-fmt build deploy release logs invoke run-local

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

run-local:
	go run ./cmd/scraper
