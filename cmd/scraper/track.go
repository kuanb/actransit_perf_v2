package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	monitoring "cloud.google.com/go/monitoring/apiv3/v2"
	monitoringpb "cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"cloud.google.com/go/storage"
	gtfs "github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
	"google.golang.org/api/googleapi"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	monitoredrespb "google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	stateObjectKey         = "state.json"
	staleThreshold         = 20 * time.Minute
	maxProbesPerTrip       = 20
	metricVehiclesInFlight = "custom.googleapis.com/actransit/vehicles_in_flight"
)

var errStateConflict = errors.New("state.json concurrent write conflict")

type stateFile struct {
	SchemaVersion int            `json:"schema_version"`
	UpdatedAt     time.Time      `json:"updated_at"`
	InFlight      []inFlightTrip `json:"in_flight"`
}

type inFlightTrip struct {
	VehicleID   string    `json:"vehicle_id"`
	RouteID     string    `json:"route_id"`
	TripID      string    `json:"trip_id"`
	ServiceDate string    `json:"service_date"`
	FirstSeenTS time.Time `json:"first_seen_ts"`
	LastSeenTS  time.Time `json:"last_seen_ts"`
	Probes      []probe   `json:"probes"`
}

type probe struct {
	TS               time.Time `json:"ts"`
	Lat              float64   `json:"lat"`
	Lon              float64   `json:"lon"`
	BearingDeg       float64   `json:"bearing_deg"`
	ReportedSpeedMps float64   `json:"reported_speed_mps"`
}

type trackStats struct {
	InFlight        int
	NewTripsStarted int
	TripsExpired    int
	ProbesAppended  int
	Conflict        bool
}

var (
	metricsClient *monitoring.MetricClient
	projectID     string
)

func trackPerformance(ctx context.Context) (trackStats, error) {
	var stats trackStats

	latestBytes, exists, err := readObject(ctx, latestObjectKey)
	if err != nil {
		return stats, fmt.Errorf("read latest: %w", err)
	}
	if !exists {
		return stats, errors.New("latest.json not yet written")
	}
	var rawEntities []json.RawMessage
	if err := json.Unmarshal(latestBytes, &rawEntities); err != nil {
		return stats, fmt.Errorf("parse latest: %w", err)
	}

	s, gen, err := readState(ctx)
	if err != nil {
		return stats, fmt.Errorf("read state: %w", err)
	}

	byVehicle := make(map[string]*inFlightTrip, len(s.InFlight))
	for i := range s.InFlight {
		byVehicle[s.InFlight[i].VehicleID] = &s.InFlight[i]
	}

	now := time.Now().UTC()
	um := protojson.UnmarshalOptions{DiscardUnknown: true}
	for _, raw := range rawEntities {
		e := &gtfs.FeedEntity{}
		if err := um.Unmarshal(raw, e); err != nil {
			continue
		}
		v := e.GetVehicle()
		if v == nil {
			continue
		}
		vid := v.GetVehicle().GetId()
		if vid == "" {
			continue
		}
		trip := v.GetTrip()
		pos := v.GetPosition()
		if pos == nil {
			continue
		}

		tripID := trip.GetTripId()
		routeID := trip.GetRouteId()
		startDate := trip.GetStartDate()

		ts := now
		if v.GetTimestamp() != 0 {
			ts = time.Unix(int64(v.GetTimestamp()), 0).UTC()
		}

		p := probe{
			TS:               ts,
			Lat:              float64(pos.GetLatitude()),
			Lon:              float64(pos.GetLongitude()),
			BearingDeg:       float64(pos.GetBearing()),
			ReportedSpeedMps: float64(pos.GetSpeed()),
		}

		existing, ok := byVehicle[vid]
		if ok && existing.TripID == tripID {
			existing.LastSeenTS = ts
			existing.Probes = append(existing.Probes, p)
			if len(existing.Probes) > maxProbesPerTrip {
				existing.Probes = existing.Probes[len(existing.Probes)-maxProbesPerTrip:]
			}
			stats.ProbesAppended++
			continue
		}

		if ok {
			stats.TripsExpired++
		}
		byVehicle[vid] = &inFlightTrip{
			VehicleID:   vid,
			RouteID:     routeID,
			TripID:      tripID,
			ServiceDate: startDate,
			FirstSeenTS: ts,
			LastSeenTS:  ts,
			Probes:      []probe{p},
		}
		stats.NewTripsStarted++
	}

	cutoff := now.Add(-staleThreshold)
	filtered := make([]inFlightTrip, 0, len(byVehicle))
	for _, t := range byVehicle {
		if t.LastSeenTS.Before(cutoff) {
			stats.TripsExpired++
			continue
		}
		filtered = append(filtered, *t)
	}

	s.InFlight = filtered
	s.UpdatedAt = now
	s.SchemaVersion = 1
	stats.InFlight = len(filtered)

	if err := writeState(ctx, s, gen); err != nil {
		if errors.Is(err, errStateConflict) {
			stats.Conflict = true
			return stats, nil
		}
		return stats, fmt.Errorf("write state: %w", err)
	}

	if err := emitVehiclesInFlight(ctx, int64(stats.InFlight)); err != nil {
		slog.Warn("emit vehicles_in_flight failed", "err", err)
	}

	return stats, nil
}

func readState(ctx context.Context) (stateFile, int64, error) {
	r, err := gcsClient.Bucket(bucketName).Object(stateObjectKey).NewReader(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return stateFile{SchemaVersion: 1}, 0, nil
		}
		return stateFile{}, 0, err
	}
	defer r.Close()
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		return stateFile{}, 0, err
	}
	var s stateFile
	if err := json.Unmarshal(buf.Bytes(), &s); err != nil {
		return stateFile{}, 0, fmt.Errorf("parse state: %w", err)
	}
	return s, r.Attrs.Generation, nil
}

func writeState(ctx context.Context, s stateFile, ifGeneration int64) error {
	payload, err := json.Marshal(s)
	if err != nil {
		return err
	}
	obj := gcsClient.Bucket(bucketName).Object(stateObjectKey)
	if ifGeneration > 0 {
		obj = obj.If(storage.Conditions{GenerationMatch: ifGeneration})
	} else {
		obj = obj.If(storage.Conditions{DoesNotExist: true})
	}
	w := obj.NewWriter(ctx)
	w.ContentType = "application/json"
	if _, err := w.Write(payload); err != nil {
		_ = w.Close()
		return err
	}
	if err := w.Close(); err != nil {
		var gErr *googleapi.Error
		if errors.As(err, &gErr) && gErr.Code == http.StatusPreconditionFailed {
			return errStateConflict
		}
		return err
	}
	return nil
}

func emitVehiclesInFlight(ctx context.Context, count int64) error {
	if metricsClient == nil || projectID == "" {
		return nil
	}
	return metricsClient.CreateTimeSeries(ctx, &monitoringpb.CreateTimeSeriesRequest{
		Name: "projects/" + projectID,
		TimeSeries: []*monitoringpb.TimeSeries{{
			Metric: &metricpb.Metric{
				Type: metricVehiclesInFlight,
			},
			Resource: &monitoredrespb.MonitoredResource{
				Type:   "global",
				Labels: map[string]string{"project_id": projectID},
			},
			Points: []*monitoringpb.Point{{
				Interval: &monitoringpb.TimeInterval{
					EndTime: timestamppb.Now(),
				},
				Value: &monitoringpb.TypedValue{
					Value: &monitoringpb.TypedValue_Int64Value{Int64Value: count},
				},
			}},
		}},
	})
}
