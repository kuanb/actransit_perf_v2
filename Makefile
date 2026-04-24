PROJECT_ID := transit-203605
REGION     := us-west1
REPO       := actransit-scraper
IMAGE      := $(REGION)-docker.pkg.dev/$(PROJECT_ID)/$(REPO)/scraper

.PHONY: tf-init tf-plan tf-apply tf-fmt build deploy logs invoke run-local

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
	cd infra && terraform apply -var "image_tag=$(TAG)"

logs:
	gcloud run services logs read $(REPO) --region $(REGION) --limit 50

invoke:
	curl -H "Authorization: Bearer $$(gcloud auth print-identity-token)" \
		"$$(cd infra && terraform output -raw scraper_url)/scrape"

run-local:
	go run ./cmd/scraper
