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
	stateObjectKey                = "state.json"
	staleThreshold                = 20 * time.Minute
	maxProbesPerTrip              = 20
	metricVehiclesInFlight        = "custom.googleapis.com/actransit/vehicles_in_flight"
	metricTripsFinalizedPerMinute = "custom.googleapis.com/actransit/trips_finalized_per_minute"
)

var errStateConflict = errors.New("state.json concurrent write conflict")

type stateFile struct {
	SchemaVersion int            `json:"schema_version"`
	UpdatedAt     time.Time      `json:"updated_at"`
	InFlight      []inFlightTrip `json:"in_flight"`
}

type inFlightTrip struct {
	VehicleID    string             `json:"vehicle_id"`
	RouteID      string             `json:"route_id"`
	TripID       string             `json:"trip_id"`
	ServiceDate  string             `json:"service_date"`
	FirstSeenTS  time.Time          `json:"first_seen_ts"`
	LastSeenTS   time.Time          `json:"last_seen_ts"`
	Probes       []probe            `json:"probes"`
	StopArrivals map[int]time.Time  `json:"stop_arrivals,omitempty"`
}

type probe struct {
	TS               time.Time `json:"ts"`
	Lat              float64   `json:"lat"`
	Lon              float64   `json:"lon"`
	BearingDeg       float64   `json:"bearing_deg"`
	ReportedSpeedMps float64   `json:"reported_speed_mps"`
	DistAlongRouteM  float64   `json:"dist_along_route_m,omitempty"`
	NearestStopSeq   int       `json:"nearest_stop_seq,omitempty"`
}

type trackStats struct {
	InFlight             int
	NewTripsStarted      int
	TripsExpired         int
	TripsCompleted       int
	ProbesAppended       int
	StopArrivalsDetected int
	TripsMissingShape    int
	RowsWrittenObs       int
	RowsWrittenProbes    int
	Conflict             bool
}

type vehicleSnapshot struct {
	VehicleID string
	RouteID   string
	TripID    string
	StartDate string
	TS        time.Time
	Lat       float64
	Lon       float64
	Bearing   float64
	SpeedMps  float64
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

	now := time.Now().UTC()
	vehicles := parseVehicleEntities(rawEntities, now)
	var preempted []inFlightTrip
	s, stats, preempted = updateInFlightState(s, vehicles, now)

	cache := ensureGTFSCache(ctx)
	if cache != nil {
		projectInFlightProbes(&s, cache, &stats)
		detectStopArrivals(&s, cache, &stats)
	}

	completed := detectCompletedTrips(&s, cache)
	stale := pruneStaleTrips(&s, now.Add(-staleThreshold))
	stats.TripsCompleted = len(completed)
	stats.TripsExpired = len(preempted) + len(completed) + len(stale)
	stats.InFlight = len(s.InFlight)

	normalEnd := append(preempted, completed...)
	if len(normalEnd) > 0 {
		obsCount, probeCount, ferr := writeFinalizedTrips(ctx, normalEnd, false, cache, now)
		stats.RowsWrittenObs += obsCount
		stats.RowsWrittenProbes += probeCount
		if ferr != nil {
			slog.Warn("finalize normal-end to BigQuery failed", "err", ferr, "trips", len(normalEnd))
		}
	}
	if len(stale) > 0 {
		obsCount, probeCount, ferr := writeFinalizedTrips(ctx, stale, true, cache, now)
		stats.RowsWrittenObs += obsCount
		stats.RowsWrittenProbes += probeCount
		if ferr != nil {
			slog.Warn("finalize stale to BigQuery failed", "err", ferr, "trips", len(stale))
		}
	}

	if err := writeState(ctx, s, gen); err != nil {
		if errors.Is(err, errStateConflict) {
			stats.Conflict = true
			return stats, nil
		}
		return stats, fmt.Errorf("write state: %w", err)
	}

	if err := emitGaugeInt(ctx, metricVehiclesInFlight, int64(stats.InFlight)); err != nil {
		slog.Warn("emit vehicles_in_flight failed", "err", err)
	}
	if err := emitGaugeInt(ctx, metricTripsFinalizedPerMinute, int64(stats.TripsExpired)); err != nil {
		slog.Warn("emit trips_finalized failed", "err", err)
	}

	return stats, nil
}

func parseVehicleEntities(raw []json.RawMessage, fallbackNow time.Time) []vehicleSnapshot {
	out := make([]vehicleSnapshot, 0, len(raw))
	um := protojson.UnmarshalOptions{DiscardUnknown: true}
	for _, r := range raw {
		e := &gtfs.FeedEntity{}
		if err := um.Unmarshal(r, e); err != nil {
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
		pos := v.GetPosition()
		if pos == nil {
			continue
		}
		trip := v.GetTrip()
		ts := fallbackNow
		if v.GetTimestamp() != 0 {
			ts = time.Unix(int64(v.GetTimestamp()), 0).UTC()
		}
		out = append(out, vehicleSnapshot{
			VehicleID: vid,
			RouteID:   trip.GetRouteId(),
			TripID:    trip.GetTripId(),
			StartDate: trip.GetStartDate(),
			TS:        ts,
			Lat:       float64(pos.GetLatitude()),
			Lon:       float64(pos.GetLongitude()),
			Bearing:   float64(pos.GetBearing()),
			SpeedMps:  float64(pos.GetSpeed()),
		})
	}
	return out
}

// updateInFlightState mutates the in-flight set by appending probes for
// continuing trips and starting new trips for new vehicles or trip-id
// changes. Returns the new state, stats for the mutation, and any trips
// that were preempted by a vehicle starting a new trip (so the caller
// can finalize them to BigQuery before they're lost).
func updateInFlightState(s stateFile, vehicles []vehicleSnapshot, now time.Time) (stateFile, trackStats, []inFlightTrip) {
	var stats trackStats
	var preempted []inFlightTrip

	byVehicle := make(map[string]*inFlightTrip, len(s.InFlight))
	for i := range s.InFlight {
		byVehicle[s.InFlight[i].VehicleID] = &s.InFlight[i]
	}

	for _, vs := range vehicles {
		p := probe{
			TS:               vs.TS,
			Lat:              vs.Lat,
			Lon:              vs.Lon,
			BearingDeg:       vs.Bearing,
			ReportedSpeedMps: vs.SpeedMps,
		}

		existing, ok := byVehicle[vs.VehicleID]
		if ok && existing.TripID == vs.TripID {
			existing.LastSeenTS = vs.TS
			if len(existing.Probes) > 0 {
				last := existing.Probes[len(existing.Probes)-1]
				if !vs.TS.After(last.TS) {
					continue
				}
			}
			existing.Probes = append(existing.Probes, p)
			if len(existing.Probes) > maxProbesPerTrip {
				existing.Probes = existing.Probes[len(existing.Probes)-maxProbesPerTrip:]
			}
			stats.ProbesAppended++
			continue
		}

		if ok {
			preempted = append(preempted, *existing)
		}
		byVehicle[vs.VehicleID] = &inFlightTrip{
			VehicleID:   vs.VehicleID,
			RouteID:     vs.RouteID,
			TripID:      vs.TripID,
			ServiceDate: vs.StartDate,
			FirstSeenTS: vs.TS,
			LastSeenTS:  vs.TS,
			Probes:      []probe{p},
		}
		stats.NewTripsStarted++
	}

	out := make([]inFlightTrip, 0, len(byVehicle))
	for _, t := range byVehicle {
		out = append(out, *t)
	}

	s.InFlight = out
	s.UpdatedAt = now
	s.SchemaVersion = 1
	stats.InFlight = len(out)
	return s, stats, preempted
}

// detectCompletedTrips removes from in-flight any trip whose final
// scheduled stop has a recorded actual arrival, and returns them for
// finalization. Pure. A trip without a known route or trip_id (no
// shape data in the cache) is left untouched — we have no way to
// know which stop is the last.
func detectCompletedTrips(s *stateFile, cache *gtfsCache) []inFlightTrip {
	if cache == nil || len(s.InFlight) == 0 {
		return nil
	}
	kept := make([]inFlightTrip, 0, len(s.InFlight))
	var completed []inFlightTrip
	for _, t := range s.InFlight {
		route, ok := cache.Routes[t.RouteID]
		if !ok {
			kept = append(kept, t)
			continue
		}
		trip, ok := route.Trips[t.TripID]
		if !ok || len(trip.StopTimes) == 0 {
			kept = append(kept, t)
			continue
		}
		lastStopSeq := trip.StopTimes[len(trip.StopTimes)-1].StopSequence
		if _, hasArrival := t.StopArrivals[lastStopSeq]; hasArrival {
			completed = append(completed, t)
			continue
		}
		kept = append(kept, t)
	}
	s.InFlight = kept
	return completed
}

// pruneStaleTrips removes any in-flight trip whose LastSeenTS is older
// than cutoff and returns the removed trips. Pure: caller writes them
// to BigQuery (chunk 4) before discarding.
func pruneStaleTrips(s *stateFile, cutoff time.Time) []inFlightTrip {
	if len(s.InFlight) == 0 {
		return nil
	}
	kept := make([]inFlightTrip, 0, len(s.InFlight))
	var stale []inFlightTrip
	for _, t := range s.InFlight {
		if t.LastSeenTS.Before(cutoff) {
			stale = append(stale, t)
			continue
		}
		kept = append(kept, t)
	}
	s.InFlight = kept
	return stale
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

// projectInFlightProbes fills DistAlongRouteM and NearestStopSeq on each
// probe in s.InFlight using the cached GTFS shapes and stop projections.
// Trips whose route or shape is unknown to the cache are left untouched
// (and counted in stats.TripsMissingShape).
func projectInFlightProbes(s *stateFile, cache *gtfsCache, stats *trackStats) {
	for i := range s.InFlight {
		t := &s.InFlight[i]
		route, ok := cache.Routes[t.RouteID]
		if !ok {
			stats.TripsMissingShape++
			continue
		}
		trip, ok := route.Trips[t.TripID]
		if !ok || trip.ShapeID == "" {
			stats.TripsMissingShape++
			continue
		}
		shape, ok := route.Shapes[trip.ShapeID]
		if !ok {
			stats.TripsMissingShape++
			continue
		}
		stops := trip.StopTimes
		for j := range t.Probes {
			p := &t.Probes[j]
			if p.DistAlongRouteM != 0 {
				continue
			}
			distAlong, _ := projectLatLonOntoShape(p.Lat, p.Lon, shape)
			p.DistAlongRouteM = distAlong
			p.NearestStopSeq = nearestStopSeq(stops, distAlong)
		}
	}
}

// detectStopArrivals scans each in-flight trip's probes (which must have
// DistAlongRouteM populated by projectInFlightProbes) and records an
// arrival timestamp for any stop the bus has now passed but that hasn't
// been recorded yet. Linearly interpolates the arrival timestamp between
// the two probes that bracket the stop.
func detectStopArrivals(s *stateFile, cache *gtfsCache, stats *trackStats) {
	for i := range s.InFlight {
		t := &s.InFlight[i]
		route, ok := cache.Routes[t.RouteID]
		if !ok {
			continue
		}
		trip, ok := route.Trips[t.TripID]
		if !ok {
			continue
		}
		if t.StopArrivals == nil {
			t.StopArrivals = make(map[int]time.Time)
		}
		for _, stop := range trip.StopTimes {
			if _, already := t.StopArrivals[stop.StopSequence]; already {
				continue
			}
			arrival, ok := arrivalForStop(t.Probes, stop.DistAlongRoute)
			if !ok {
				continue
			}
			t.StopArrivals[stop.StopSequence] = arrival
			stats.StopArrivalsDetected++
		}
	}
}

// arrivalForStop returns the interpolated timestamp at which the bus
// was observed crossing a stop located at stopDist along the route.
// Returns false unless we have a "before" probe (dist < stopDist) AND a
// later probe at or past stopDist — i.e., we actually witnessed the
// crossing. A single probe alone, even past the stop, is not enough:
// the bus may have crossed long before we started observing it.
func arrivalForStop(probes []probe, stopDist float64) (time.Time, bool) {
	for i, p := range probes {
		if p.DistAlongRouteM < stopDist {
			continue
		}
		if i == 0 {
			return time.Time{}, false
		}
		prev := probes[i-1]
		if prev.DistAlongRouteM >= stopDist {
			return time.Time{}, false
		}
		span := p.DistAlongRouteM - prev.DistAlongRouteM
		if span <= 0 {
			return p.TS, true
		}
		frac := (stopDist - prev.DistAlongRouteM) / span
		dt := p.TS.Sub(prev.TS)
		offset := time.Duration(float64(dt) * frac)
		return prev.TS.Add(offset), true
	}
	return time.Time{}, false
}

// emitGaugeInt writes a single int64 gauge data point to Cloud Monitoring.
// Used for both vehicles_in_flight and trips_finalized_per_minute.
func emitGaugeInt(ctx context.Context, metricType string, value int64) error {
	if metricsClient == nil || projectID == "" {
		return nil
	}
	return metricsClient.CreateTimeSeries(ctx, &monitoringpb.CreateTimeSeriesRequest{
		Name: "projects/" + projectID,
		TimeSeries: []*monitoringpb.TimeSeries{{
			Metric: &metricpb.Metric{
				Type: metricType,
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
					Value: &monitoringpb.TypedValue_Int64Value{Int64Value: value},
				},
			}},
		}},
	})
}
