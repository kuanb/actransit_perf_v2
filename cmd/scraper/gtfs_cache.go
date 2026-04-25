package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

const (
	gtfsCachePrefix    = "gtfs/processed/"
	gtfsCacheCheckEvery = 100
)

type gtfsCache struct {
	Routes     map[string]*processedGTFSRoute
	FeedHash   string
	Generation int64
	LoadedAt   time.Time
}

var (
	gtfsCacheRef      atomic.Pointer[gtfsCache]
	gtfsCallCounter   atomic.Int64
)

func ensureGTFSCache(ctx context.Context) *gtfsCache {
	current := gtfsCacheRef.Load()
	count := gtfsCallCounter.Add(1)

	if current != nil && count%gtfsCacheCheckEvery != 1 {
		return current
	}

	if current != nil {
		attrs, err := gcsClient.Bucket(bucketName).Object(gtfsCurrentKey).Attrs(ctx)
		if err == nil && attrs.Generation == current.Generation {
			return current
		}
	}

	fresh, err := loadGTFSCache(ctx)
	if err != nil {
		slog.Warn("gtfs cache load failed; keeping previous", "err", err)
		return current
	}
	gtfsCacheRef.Store(fresh)
	slog.Info("gtfs cache loaded",
		"routes", len(fresh.Routes),
		"generation", fresh.Generation,
		"feed_hash", truncateHash(fresh.FeedHash),
	)
	return fresh
}

func loadGTFSCache(ctx context.Context) (*gtfsCache, error) {
	bucket := gcsClient.Bucket(bucketName)
	cache := &gtfsCache{
		Routes:   make(map[string]*processedGTFSRoute),
		LoadedAt: time.Now().UTC(),
	}

	it := bucket.Objects(ctx, &storage.Query{Prefix: gtfsCachePrefix})
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("list %s: %w", gtfsCachePrefix, err)
		}
		rc, err := bucket.Object(attrs.Name).NewReader(ctx)
		if err != nil {
			return nil, fmt.Errorf("open %s: %w", attrs.Name, err)
		}
		body, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", attrs.Name, err)
		}
		var route processedGTFSRoute
		if err := json.Unmarshal(body, &route); err != nil {
			return nil, fmt.Errorf("decode %s: %w", attrs.Name, err)
		}
		cache.Routes[route.RouteID] = &route
		cache.FeedHash = route.FeedHash
	}

	if curAttrs, err := bucket.Object(gtfsCurrentKey).Attrs(ctx); err == nil {
		cache.Generation = curAttrs.Generation
	}
	return cache, nil
}

// nearestStopSeq returns the stop_sequence of the stop whose
// dist_along_route_m is closest to dist. Stops must be sorted by
// dist_along_route_m. Returns 0 if stops is empty.
func nearestStopSeq(stops []gtfsStopTime, dist float64) int {
	if len(stops) == 0 {
		return 0
	}
	best := stops[0]
	bestDelta := abs(best.DistAlongRoute - dist)
	for _, st := range stops[1:] {
		d := abs(st.DistAlongRoute - dist)
		if d < bestDelta {
			best = st
			bestDelta = d
		}
	}
	return best.StopSequence
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

func truncateHash(h string) string {
	if len(h) > 12 {
		return h[:12]
	}
	return h
}
