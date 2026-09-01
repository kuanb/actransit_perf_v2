package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
)

const (
	apiRequestBQTable         = "api_request_observations"
	apiRequestHourlyBQTable   = "api_request_hourly"
	apiSourceVehicleLocations = "vehicle_locations"
	apiSourceRidership        = "ridership"
	apiRequestTimeout         = 30 * time.Second
)

var acTransitHTTPClient = &http.Client{Timeout: apiRequestTimeout}

type apiRequestObservation struct {
	ServiceDate   civil.Date          `bigquery:"service_date"`
	ObservedAt    time.Time           `bigquery:"observed_at"`
	Source        string              `bigquery:"source"`
	Endpoint      string              `bigquery:"endpoint"`
	LatencyMS     float64             `bigquery:"latency_ms"`
	StatusCode    bigquery.NullInt64  `bigquery:"status_code"`
	Success       bool                `bigquery:"success"`
	Outcome       string              `bigquery:"outcome"`
	ResponseBytes bigquery.NullInt64  `bigquery:"response_bytes"`
	ErrorMessage  bigquery.NullString `bigquery:"error_message"`
	IngestedAt    time.Time           `bigquery:"ingested_at"`
}

func fetchObservedAPI(ctx context.Context, client *http.Client, source, rawURL string) (body []byte, observation apiRequestObservation, err error) {
	startedAt := time.Now()
	observation = apiRequestObservation{
		ServiceDate: apiRequestServiceDate(startedAt),
		ObservedAt:  startedAt.UTC(),
		Source:      source,
		Endpoint:    apiEndpointName(rawURL),
		Outcome:     "request_error",
	}
	defer func() {
		observation.LatencyMS = float64(time.Since(startedAt)) / float64(time.Millisecond)
	}()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		setAPIRequestFailure(&observation, "request_error", err)
		return nil, observation, err
	}
	resp, err := client.Do(req)
	if err != nil {
		err = sanitizeHTTPError(err)
		setAPIRequestFailure(&observation, apiTransportOutcome(err), err)
		return nil, observation, err
	}
	defer resp.Body.Close()

	observation.StatusCode = bigquery.NullInt64{Int64: int64(resp.StatusCode), Valid: true}
	body, err = io.ReadAll(resp.Body)
	observation.ResponseBytes = bigquery.NullInt64{Int64: int64(len(body)), Valid: true}
	if err != nil {
		err = sanitizeHTTPError(err)
		outcome := "body_read_error"
		if apiTransportOutcome(err) == "timeout" {
			outcome = "timeout"
		}
		setAPIRequestFailure(&observation, outcome, err)
		return body, observation, err
	}
	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("upstream %s: %s", req.URL.Path, resp.Status)
		setAPIRequestFailure(&observation, apiHTTPOutcome(resp.StatusCode), err)
		return body, observation, err
	}

	observation.Success = true
	observation.Outcome = "success"
	return body, observation, nil
}

func setAPIRequestFailure(observation *apiRequestObservation, outcome string, err error) {
	observation.Success = false
	observation.Outcome = outcome
	observation.ErrorMessage = bigquery.NullString{StringVal: err.Error(), Valid: true}
}

func apiTransportOutcome(err error) string {
	var netErr net.Error
	if errors.Is(err, context.DeadlineExceeded) || errors.As(err, &netErr) && netErr.Timeout() {
		return "timeout"
	}
	return "transport_error"
}

func apiHTTPOutcome(statusCode int) string {
	switch {
	case statusCode >= 400 && statusCode < 500:
		return "http_4xx"
	case statusCode >= 500 && statusCode < 600:
		return "http_5xx"
	default:
		return "http_other"
	}
}

func apiEndpointName(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "invalid_url"
	}
	return parsed.Host + parsed.EscapedPath()
}

func apiRequestServiceDate(observedAt time.Time) civil.Date {
	loc, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		loc = time.UTC
	}
	return civil.DateOf(observedAt.In(loc))
}

func recordAPIRequest(ctx context.Context, observation apiRequestObservation) {
	if err := writeAPIRequestObservation(ctx, observation); err != nil {
		slog.Warn("write api request observation failed",
			"source", observation.Source,
			"observed_at", observation.ObservedAt,
			"status_code", observation.StatusCode,
			"outcome", observation.Outcome,
			"err", err,
		)
	}
}

func writeAPIRequestObservation(ctx context.Context, observation apiRequestObservation) error {
	if apiRequestBQWriter == nil {
		return nil
	}
	observation.IngestedAt = time.Now().UTC()
	row := map[string]any{
		"service_date": bqDateValue(observation.ServiceDate),
		"observed_at":  observation.ObservedAt.UnixMicro(),
		"source":       observation.Source,
		"endpoint":     observation.Endpoint,
		"latency_ms":   observation.LatencyMS,
		"success":      observation.Success,
		"outcome":      observation.Outcome,
		"ingested_at":  observation.IngestedAt.UnixMicro(),
	}
	if observation.StatusCode.Valid {
		row["status_code"] = observation.StatusCode.Int64
	}
	if observation.ResponseBytes.Valid {
		row["response_bytes"] = observation.ResponseBytes.Int64
	}
	if observation.ErrorMessage.Valid {
		row["error_message"] = observation.ErrorMessage.StringVal
	}
	return apiRequestBQWriter.Append(ctx, []map[string]any{row})
}
