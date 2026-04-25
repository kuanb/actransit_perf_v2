package main

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type processedGTFSRoute struct {
	RouteID     string                  `json:"route_id"`
	FeedHash    string                  `json:"feed_hash"`
	GeneratedAt time.Time               `json:"generated_at"`
	Shapes      map[string][][3]float64 `json:"shapes"`
	Stops       map[string]gtfsStop     `json:"stops"`
	Trips       map[string]gtfsTrip     `json:"trips"`
}

type gtfsStop struct {
	StopID   string  `json:"stop_id"`
	StopName string  `json:"stop_name"`
	Lat      float64 `json:"lat"`
	Lon      float64 `json:"lon"`
}

type gtfsTrip struct {
	TripID      string         `json:"trip_id"`
	ShapeID     string         `json:"shape_id"`
	ServiceID   string         `json:"service_id"`
	DirectionID int            `json:"direction_id"`
	StopTimes   []gtfsStopTime `json:"stop_times"`
}

type gtfsStopTime struct {
	StopSequence   int     `json:"stop_sequence"`
	StopID         string  `json:"stop_id"`
	ArrivalTime    string  `json:"arrival_time"`
	DepartureTime  string  `json:"departure_time"`
	DistAlongRoute float64 `json:"dist_along_route_m"`
}

type gtfsLookups struct {
	stops       map[string]gtfsStop
	shapes      map[string][][3]float64
	trips       map[string]*gtfsTrip
	tripToRoute map[string]string
}

func processGTFS(zipBytes []byte, feedHash string) (map[string]*processedGTFSRoute, error) {
	zr, err := zip.NewReader(bytes.NewReader(zipBytes), int64(len(zipBytes)))
	if err != nil {
		return nil, fmt.Errorf("open zip: %w", err)
	}

	g := &gtfsLookups{
		stops:       make(map[string]gtfsStop),
		shapes:      make(map[string][][3]float64),
		trips:       make(map[string]*gtfsTrip),
		tripToRoute: make(map[string]string),
	}

	if err := streamStops(zr, g); err != nil {
		return nil, fmt.Errorf("stops.txt: %w", err)
	}
	if err := streamShapes(zr, g); err != nil {
		return nil, fmt.Errorf("shapes.txt: %w", err)
	}
	if err := streamTrips(zr, g); err != nil {
		return nil, fmt.Errorf("trips.txt: %w", err)
	}
	if err := streamStopTimes(zr, g); err != nil {
		return nil, fmt.Errorf("stop_times.txt: %w", err)
	}

	routes := groupIntoRoutes(g)
	now := time.Now().UTC()
	for _, r := range routes {
		r.FeedHash = feedHash
		r.GeneratedAt = now
	}
	return routes, nil
}

type csvIter struct {
	rc        io.ReadCloser
	cr        *csv.Reader
	headerIdx map[string]int
}

func openCSV(zr *zip.Reader, name string) (*csvIter, error) {
	var file *zip.File
	for _, f := range zr.File {
		if f.Name == name {
			file = f
			break
		}
	}
	if file == nil {
		return nil, fmt.Errorf("missing %s", name)
	}
	rc, err := file.Open()
	if err != nil {
		return nil, err
	}
	cr := csv.NewReader(rc)
	cr.FieldsPerRecord = -1
	cr.TrimLeadingSpace = true
	cr.ReuseRecord = true

	headers, err := cr.Read()
	if err != nil {
		rc.Close()
		return nil, err
	}
	if len(headers) > 0 {
		headers[0] = strings.TrimPrefix(headers[0], "\ufeff")
	}
	idx := make(map[string]int, len(headers))
	for i, h := range headers {
		idx[h] = i
	}
	return &csvIter{rc: rc, cr: cr, headerIdx: idx}, nil
}

func (c *csvIter) close() error { return c.rc.Close() }

func (c *csvIter) read() ([]string, error) { return c.cr.Read() }

func (c *csvIter) col(row []string, name string) string {
	i, ok := c.headerIdx[name]
	if !ok || i >= len(row) {
		return ""
	}
	return row[i]
}

func streamStops(zr *zip.Reader, g *gtfsLookups) error {
	it, err := openCSV(zr, "stops.txt")
	if err != nil {
		return err
	}
	defer it.close()
	for {
		row, err := it.read()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		sid := it.col(row, "stop_id")
		if sid == "" {
			continue
		}
		lat, _ := strconv.ParseFloat(it.col(row, "stop_lat"), 64)
		lon, _ := strconv.ParseFloat(it.col(row, "stop_lon"), 64)
		g.stops[sid] = gtfsStop{
			StopID:   sid,
			StopName: it.col(row, "stop_name"),
			Lat:      lat,
			Lon:      lon,
		}
	}
}

type rawShapePt struct {
	seq int
	lat float64
	lon float64
}

func streamShapes(zr *zip.Reader, g *gtfsLookups) error {
	it, err := openCSV(zr, "shapes.txt")
	if err != nil {
		return err
	}
	defer it.close()

	bySeq := make(map[string][]rawShapePt)
	for {
		row, err := it.read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		shapeID := it.col(row, "shape_id")
		if shapeID == "" {
			continue
		}
		seq, _ := strconv.Atoi(it.col(row, "shape_pt_sequence"))
		lat, _ := strconv.ParseFloat(it.col(row, "shape_pt_lat"), 64)
		lon, _ := strconv.ParseFloat(it.col(row, "shape_pt_lon"), 64)
		bySeq[shapeID] = append(bySeq[shapeID], rawShapePt{seq: seq, lat: lat, lon: lon})
	}

	for shapeID, pts := range bySeq {
		sort.Slice(pts, func(i, j int) bool { return pts[i].seq < pts[j].seq })
		out := make([][3]float64, len(pts))
		cum := 0.0
		for i, p := range pts {
			if i > 0 {
				cum += haversineMeters(out[i-1][0], out[i-1][1], p.lat, p.lon)
			}
			out[i] = [3]float64{p.lat, p.lon, cum}
		}
		g.shapes[shapeID] = out
	}
	return nil
}

func streamTrips(zr *zip.Reader, g *gtfsLookups) error {
	it, err := openCSV(zr, "trips.txt")
	if err != nil {
		return err
	}
	defer it.close()
	for {
		row, err := it.read()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		tripID := it.col(row, "trip_id")
		if tripID == "" {
			continue
		}
		dirID, _ := strconv.Atoi(it.col(row, "direction_id"))
		g.trips[tripID] = &gtfsTrip{
			TripID:      tripID,
			ShapeID:     it.col(row, "shape_id"),
			ServiceID:   it.col(row, "service_id"),
			DirectionID: dirID,
		}
		g.tripToRoute[tripID] = it.col(row, "route_id")
	}
}

func streamStopTimes(zr *zip.Reader, g *gtfsLookups) error {
	it, err := openCSV(zr, "stop_times.txt")
	if err != nil {
		return err
	}
	defer it.close()
	for {
		row, err := it.read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		tripID := it.col(row, "trip_id")
		trip, ok := g.trips[tripID]
		if !ok {
			continue
		}
		seq, _ := strconv.Atoi(it.col(row, "stop_sequence"))
		trip.StopTimes = append(trip.StopTimes, gtfsStopTime{
			StopSequence:  seq,
			StopID:        it.col(row, "stop_id"),
			ArrivalTime:   it.col(row, "arrival_time"),
			DepartureTime: it.col(row, "departure_time"),
		})
	}

	for _, trip := range g.trips {
		if len(trip.StopTimes) == 0 {
			continue
		}
		sort.Slice(trip.StopTimes, func(i, j int) bool {
			return trip.StopTimes[i].StopSequence < trip.StopTimes[j].StopSequence
		})
	}

	type shapeStopKey struct {
		shapeID string
		stopID  string
	}
	needed := make(map[shapeStopKey]struct{})
	for _, trip := range g.trips {
		if trip.ShapeID == "" {
			continue
		}
		for _, st := range trip.StopTimes {
			needed[shapeStopKey{trip.ShapeID, st.StopID}] = struct{}{}
		}
	}
	projections := make(map[shapeStopKey]float64, len(needed))
	for k := range needed {
		shape := g.shapes[k.shapeID]
		stop, ok := g.stops[k.stopID]
		if !ok {
			continue
		}
		distAlong, _ := projectLatLonOntoShape(stop.Lat, stop.Lon, shape)
		projections[k] = distAlong
	}
	for _, trip := range g.trips {
		if trip.ShapeID == "" {
			continue
		}
		for i := range trip.StopTimes {
			if d, ok := projections[shapeStopKey{trip.ShapeID, trip.StopTimes[i].StopID}]; ok {
				trip.StopTimes[i].DistAlongRoute = d
			}
		}
	}
	return nil
}

func groupIntoRoutes(g *gtfsLookups) map[string]*processedGTFSRoute {
	routes := make(map[string]*processedGTFSRoute)
	for tripID, trip := range g.trips {
		routeID := g.tripToRoute[tripID]
		if routeID == "" {
			continue
		}
		rd, ok := routes[routeID]
		if !ok {
			rd = &processedGTFSRoute{
				RouteID: routeID,
				Shapes:  make(map[string][][3]float64),
				Stops:   make(map[string]gtfsStop),
				Trips:   make(map[string]gtfsTrip),
			}
			routes[routeID] = rd
		}
		rd.Trips[tripID] = *trip
		if trip.ShapeID != "" {
			if shape, ok := g.shapes[trip.ShapeID]; ok {
				rd.Shapes[trip.ShapeID] = shape
			}
		}
		for _, st := range trip.StopTimes {
			if stop, ok := g.stops[st.StopID]; ok {
				rd.Stops[st.StopID] = stop
			}
		}
	}
	return routes
}

func writePerRouteJSONs(ctx context.Context, routes map[string]*processedGTFSRoute) error {
	var wg sync.WaitGroup
	sem := make(chan struct{}, 4)
	var firstErr error
	var errMu sync.Mutex

	recordErr := func(err error) {
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		errMu.Unlock()
	}

	for routeID, r := range routes {
		wg.Add(1)
		sem <- struct{}{}
		go func(rid string, r *processedGTFSRoute) {
			defer wg.Done()
			defer func() { <-sem }()

			payload, err := json.Marshal(r)
			if err != nil {
				recordErr(fmt.Errorf("marshal route %s: %w", rid, err))
				return
			}
			key := fmt.Sprintf("gtfs/processed/route_%s.json", sanitizeRouteID(rid))
			if err := writeObject(ctx, key, payload); err != nil {
				recordErr(fmt.Errorf("write %s: %w", key, err))
			}
		}(routeID, r)
	}

	wg.Wait()
	return firstErr
}

func sanitizeRouteID(rid string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-' || r == '_':
			return r
		}
		return '_'
	}, rid)
}
