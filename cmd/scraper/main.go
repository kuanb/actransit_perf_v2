package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	monitoring "cloud.google.com/go/monitoring/apiv3/v2"
	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	secretmanagerpb "cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"cloud.google.com/go/storage"
	gtfs "github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	maxHistory          = 10
	latestObjectKey     = "latest.json"
	historyObjectKey    = "history.json"
	routeStopsObjectKey = "route_stops.json"
	gtfsCurrentKey      = "gtfs/current.zip"
	gtfsHashMetaKey     = "feed-hash"
	feedURLTemplate     = "https://api.actransit.org/transit/gtfsrt/vehicles?token=%s"
	allStopsURLTemplate = "https://api.actransit.org/transit/actrealtime/allstops?rt=%s&token=%s"
	gtfsURLTemplate     = "https://api.actransit.org/transit/gtfs/download?token=%s"
)

var (
	gcsClient  *storage.Client
	bucketName string

	apiToken  *tokenCache
	gtfsToken *tokenCache
)

type tokenCache struct {
	name  string
	once  sync.Once
	value string
	err   error
}

func (t *tokenCache) Get(ctx context.Context) (string, error) {
	t.once.Do(func() {
		c, err := secretmanager.NewClient(ctx)
		if err != nil {
			t.err = err
			return
		}
		defer c.Close()
		resp, err := c.AccessSecretVersion(ctx, &secretmanagerpb.AccessSecretVersionRequest{Name: t.name})
		if err != nil {
			t.err = fmt.Errorf("access %s: %w", t.name, err)
			return
		}
		t.value = string(resp.Payload.Data)
	})
	return t.value, t.err
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	bucketName = os.Getenv("CACHE_BUCKET")
	apiSecretName := os.Getenv("SECRET_NAME")
	gtfsSecretName := os.Getenv("GTFS_SECRET_NAME")
	projectID = os.Getenv("PROJECT_ID")
	if bucketName == "" || apiSecretName == "" || gtfsSecretName == "" || projectID == "" {
		slog.Error("missing required env",
			"CACHE_BUCKET", bucketName,
			"SECRET_NAME", apiSecretName,
			"GTFS_SECRET_NAME", gtfsSecretName,
			"PROJECT_ID", projectID,
		)
		os.Exit(1)
	}
	apiToken = &tokenCache{name: apiSecretName}
	gtfsToken = &tokenCache{name: gtfsSecretName}

	ctx := context.Background()
	c, err := storage.NewClient(ctx)
	if err != nil {
		slog.Error("storage.NewClient failed", "err", err)
		os.Exit(1)
	}
	defer c.Close()
	gcsClient = c

	mc, err := monitoring.NewMetricClient(ctx)
	if err != nil {
		slog.Error("monitoring.NewMetricClient failed", "err", err)
		os.Exit(1)
	}
	defer mc.Close()
	metricsClient = mc

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	http.HandleFunc("/scrape", handleScrape)
	http.HandleFunc("/refresh-stops", handleRefreshStops)
	http.HandleFunc("/refresh-gtfs", handleRefreshGTFS)
	http.HandleFunc("/track-performance", handleTrackPerformance)
	http.HandleFunc("/", handleHealth)

	slog.Info("listening", "port", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		slog.Error("server failed", "err", err)
		os.Exit(1)
	}
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "ok")
}

func handleScrape(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if err := scrape(r.Context()); err != nil {
		slog.Error("scrape failed", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	slog.Info("scrape ok", "duration_ms", time.Since(start).Milliseconds())
	fmt.Fprintln(w, "ok")
}

func handleRefreshStops(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	stats, err := refreshStops(r.Context())
	if err != nil {
		slog.Error("refresh-stops failed", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	slog.Info("refresh-stops ok",
		"duration_ms", time.Since(start).Milliseconds(),
		"routes_total", stats.Total,
		"routes_failed", stats.Failed,
	)
	fmt.Fprintln(w, "ok")
}

func handleTrackPerformance(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	stats, err := trackPerformance(r.Context())
	if err != nil {
		slog.Error("track-performance failed", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if stats.Conflict {
		slog.Info("track-performance conflict",
			"duration_ms", time.Since(start).Milliseconds(),
			"note", "another writer updated state.json first; will reconcile next minute",
		)
	} else {
		slog.Info("track-performance ok",
			"duration_ms", time.Since(start).Milliseconds(),
			"in_flight", stats.InFlight,
			"new_trips", stats.NewTripsStarted,
			"trips_expired", stats.TripsExpired,
			"probes_appended", stats.ProbesAppended,
		)
	}
	fmt.Fprintln(w, "ok")
}

func handleRefreshGTFS(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	stats, err := refreshGTFS(r.Context())
	if err != nil {
		slog.Error("refresh-gtfs failed", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if stats.Changed {
		slog.Info("refresh-gtfs updated",
			"duration_ms", time.Since(start).Milliseconds(),
			"bytes", stats.Bytes,
			"new_hash", stats.NewHash[:12],
			"archive_key", stats.ArchiveKey,
			"routes_processed", stats.RoutesProcessed,
		)
	} else {
		slog.Info("refresh-gtfs unchanged",
			"duration_ms", time.Since(start).Milliseconds(),
			"bytes", stats.Bytes,
			"hash", stats.NewHash[:12],
		)
	}
	fmt.Fprintln(w, "ok")
}

func scrape(ctx context.Context) error {
	token, err := apiToken.Get(ctx)
	if err != nil {
		return fmt.Errorf("get token: %w", err)
	}

	body, err := httpGet(ctx, fmt.Sprintf(feedURLTemplate, token))
	if err != nil {
		return fmt.Errorf("fetch feed: %w", err)
	}

	feed := &gtfs.FeedMessage{}
	if err := proto.Unmarshal(body, feed); err != nil {
		return fmt.Errorf("decode protobuf: %w", err)
	}

	vehiclesJSON, err := entitiesToJSON(feed.Entity)
	if err != nil {
		return fmt.Errorf("marshal entities: %w", err)
	}

	prevLatest, _, err := readObject(ctx, latestObjectKey)
	if err != nil {
		return fmt.Errorf("read previous latest: %w", err)
	}

	if err := writeObject(ctx, latestObjectKey, vehiclesJSON); err != nil {
		return fmt.Errorf("write latest: %w", err)
	}

	if err := updateHistory(ctx, prevLatest); err != nil {
		return fmt.Errorf("update history: %w", err)
	}

	return nil
}

type refreshStats struct {
	Total  int
	Failed int
}

type routeStopsEntry struct {
	RouteName      string          `json:"routeName"`
	ProcessedStops json.RawMessage `json:"processedStops"`
}

func refreshStops(ctx context.Context) (refreshStats, error) {
	var stats refreshStats

	token, err := apiToken.Get(ctx)
	if err != nil {
		return stats, fmt.Errorf("get token: %w", err)
	}

	latestBytes, exists, err := readObject(ctx, latestObjectKey)
	if err != nil {
		return stats, fmt.Errorf("read latest: %w", err)
	}
	if !exists {
		return stats, errors.New("latest.json not yet written; run /scrape first")
	}

	var rawEntities []json.RawMessage
	if err := json.Unmarshal(latestBytes, &rawEntities); err != nil {
		return stats, fmt.Errorf("parse latest: %w", err)
	}

	routeSet := make(map[string]struct{})
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
		t := v.GetTrip()
		if t == nil {
			continue
		}
		if rid := t.GetRouteId(); rid != "" {
			routeSet[rid] = struct{}{}
		}
	}

	out := make([]routeStopsEntry, 0, len(routeSet))
	for routeName := range routeSet {
		stats.Total++
		url := fmt.Sprintf(allStopsURLTemplate, routeName, token)
		body, err := httpGet(ctx, url)
		if err != nil {
			slog.Warn("allstops fetch failed", "route", routeName, "err", err)
			stats.Failed++
			out = append(out, routeStopsEntry{RouteName: routeName, ProcessedStops: json.RawMessage("null")})
			continue
		}
		out = append(out, routeStopsEntry{RouteName: routeName, ProcessedStops: body})
	}

	payload, err := json.Marshal(out)
	if err != nil {
		return stats, err
	}
	if err := writeObject(ctx, routeStopsObjectKey, payload); err != nil {
		return stats, fmt.Errorf("write route_stops: %w", err)
	}
	return stats, nil
}

type gtfsStats struct {
	Bytes           int
	PrevHash        string
	NewHash         string
	Changed         bool
	ArchiveKey      string
	RoutesProcessed int
}

func refreshGTFS(ctx context.Context) (gtfsStats, error) {
	var stats gtfsStats

	token, err := gtfsToken.Get(ctx)
	if err != nil {
		return stats, fmt.Errorf("get gtfs token: %w", err)
	}

	body, err := httpGet(ctx, fmt.Sprintf(gtfsURLTemplate, token))
	if err != nil {
		return stats, fmt.Errorf("download gtfs: %w", err)
	}
	stats.Bytes = len(body)

	sum := sha256.Sum256(body)
	stats.NewHash = hex.EncodeToString(sum[:])

	attrs, err := gcsClient.Bucket(bucketName).Object(gtfsCurrentKey).Attrs(ctx)
	switch {
	case err == nil:
		stats.PrevHash = attrs.Metadata[gtfsHashMetaKey]
		if stats.PrevHash == stats.NewHash {
			return stats, nil
		}
	case errors.Is(err, storage.ErrObjectNotExist):
		// first run: nothing to compare
	default:
		return stats, fmt.Errorf("get current attrs: %w", err)
	}

	stats.Changed = true
	stats.ArchiveKey = fmt.Sprintf("gtfs/%s.zip", time.Now().UTC().Format("20060102T150405Z"))

	routes, err := processGTFS(body, stats.NewHash)
	if err != nil {
		return stats, fmt.Errorf("process gtfs: %w", err)
	}
	stats.RoutesProcessed = len(routes)

	if err := writeGTFSObject(ctx, stats.ArchiveKey, body, stats.NewHash); err != nil {
		return stats, fmt.Errorf("write archive: %w", err)
	}
	if err := writeGTFSObject(ctx, gtfsCurrentKey, body, stats.NewHash); err != nil {
		return stats, fmt.Errorf("write current: %w", err)
	}
	if err := writePerRouteJSONs(ctx, routes); err != nil {
		return stats, fmt.Errorf("write per-route: %w", err)
	}
	return stats, nil
}

func writeGTFSObject(ctx context.Context, key string, data []byte, hash string) error {
	w := gcsClient.Bucket(bucketName).Object(key).NewWriter(ctx)
	w.ContentType = "application/zip"
	w.Metadata = map[string]string{gtfsHashMetaKey: hash}
	if _, err := w.Write(data); err != nil {
		_ = w.Close()
		return err
	}
	return w.Close()
}

func httpGet(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("upstream %s: %s", url, resp.Status)
	}
	return io.ReadAll(resp.Body)
}

func entitiesToJSON(entities []*gtfs.FeedEntity) ([]byte, error) {
	m := protojson.MarshalOptions{UseProtoNames: false, EmitUnpopulated: false}
	out := make([]json.RawMessage, 0, len(entities))
	for _, e := range entities {
		j, err := m.Marshal(e)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return json.Marshal(out)
}

func updateHistory(ctx context.Context, newest []byte) error {
	if newest == nil {
		return nil
	}
	historyBytes, exists, err := readObject(ctx, historyObjectKey)
	if err != nil {
		return err
	}
	var history []json.RawMessage
	if exists {
		if err := json.Unmarshal(historyBytes, &history); err != nil {
			return fmt.Errorf("parse history: %w", err)
		}
	}
	history = append([]json.RawMessage{newest}, history...)
	if len(history) > maxHistory {
		history = history[:maxHistory]
	}
	out, err := json.Marshal(history)
	if err != nil {
		return err
	}
	return writeObject(ctx, historyObjectKey, out)
}

func writeObject(ctx context.Context, key string, data []byte) error {
	w := gcsClient.Bucket(bucketName).Object(key).NewWriter(ctx)
	w.ContentType = "application/json"
	if _, err := w.Write(data); err != nil {
		_ = w.Close()
		return err
	}
	return w.Close()
}

func readObject(ctx context.Context, key string) ([]byte, bool, error) {
	r, err := gcsClient.Bucket(bucketName).Object(key).NewReader(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return nil, false, nil
		}
		return nil, false, err
	}
	defer r.Close()
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		return nil, false, err
	}
	return buf.Bytes(), true, nil
}
