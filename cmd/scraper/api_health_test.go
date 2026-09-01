package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestFetchObservedAPISuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("payload"))
	}))
	defer server.Close()

	body, observation, err := fetchObservedAPI(context.Background(), server.Client(), apiSourceVehicleLocations, server.URL+"/vehicles?token=secret")
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "payload" {
		t.Fatalf("body = %q, want payload", body)
	}
	if !observation.Success || observation.Outcome != "success" {
		t.Fatalf("health = success:%t outcome:%q", observation.Success, observation.Outcome)
	}
	if !observation.StatusCode.Valid || observation.StatusCode.Int64 != http.StatusOK {
		t.Fatalf("status = %+v, want 200", observation.StatusCode)
	}
	if !observation.ResponseBytes.Valid || observation.ResponseBytes.Int64 != int64(len(body)) {
		t.Fatalf("response bytes = %+v, want %d", observation.ResponseBytes, len(body))
	}
	if observation.LatencyMS < 0 {
		t.Fatalf("latency_ms = %f, want non-negative", observation.LatencyMS)
	}
	if strings.Contains(observation.Endpoint, "secret") || strings.Contains(observation.Endpoint, "?") {
		t.Fatalf("endpoint leaked query string: %q", observation.Endpoint)
	}
}

func TestFetchObservedAPIHTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "rate limited", http.StatusTooManyRequests)
	}))
	defer server.Close()

	_, observation, err := fetchObservedAPI(context.Background(), server.Client(), apiSourceRidership, server.URL+"/ridership")
	if err == nil {
		t.Fatal("expected HTTP error")
	}
	if observation.Success || observation.Outcome != "http_4xx" {
		t.Fatalf("health = success:%t outcome:%q", observation.Success, observation.Outcome)
	}
	if !observation.StatusCode.Valid || observation.StatusCode.Int64 != http.StatusTooManyRequests {
		t.Fatalf("status = %+v, want 429", observation.StatusCode)
	}
	if !observation.ErrorMessage.Valid || !strings.Contains(observation.ErrorMessage.StringVal, "429") {
		t.Fatalf("error message = %+v, want 429", observation.ErrorMessage)
	}
}

func TestFetchObservedAPITimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(50 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	client := server.Client()
	client.Timeout = 5 * time.Millisecond

	_, observation, err := fetchObservedAPI(context.Background(), client, apiSourceRidership, server.URL+"/ridership?token=secret")
	if err == nil {
		t.Fatal("expected timeout")
	}
	if observation.Success || observation.Outcome != "timeout" {
		t.Fatalf("health = success:%t outcome:%q", observation.Success, observation.Outcome)
	}
	if observation.StatusCode.Valid {
		t.Fatalf("status = %+v, want null", observation.StatusCode)
	}
	if strings.Contains(observation.Endpoint, "secret") || strings.Contains(observation.ErrorMessage.StringVal, "secret") {
		t.Fatalf("timeout observation leaked token: endpoint=%q error=%q", observation.Endpoint, observation.ErrorMessage.StringVal)
	}
}

func TestAPIHTTPOutcome(t *testing.T) {
	tests := map[int]string{
		302: "http_other",
		404: "http_4xx",
		503: "http_5xx",
	}
	for status, want := range tests {
		if got := apiHTTPOutcome(status); got != want {
			t.Errorf("apiHTTPOutcome(%d) = %q, want %q", status, got, want)
		}
	}
}
