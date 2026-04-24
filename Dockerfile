FROM golang:1.25-alpine AS builder
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/scraper ./cmd/scraper

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /out/scraper /scraper
USER nonroot:nonroot
ENTRYPOINT ["/scraper"]
