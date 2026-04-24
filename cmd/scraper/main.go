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
	"os"
	"sync"
	"time"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	secretmanagerpb "cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"cloud.google.com/go/storage"
	gtfs "github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	maxHistory       = 10
	latestObjectKey  = "latest.json"
	historyObjectKey = "history.json"
	feedURLTemplate  = "https://api.actransit.org/transit/gtfsrt/vehicles?token=%s"
)

var (
	gcsClient  *storage.Client
	bucketName string
	secretName string

	secretOnce  sync.Once
	secretValue string
	secretErr   error
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	bucketName = os.Getenv("CACHE_BUCKET")
	secretName = os.Getenv("SECRET_NAME")
	if bucketName == "" || secretName == "" {
		slog.Error("missing required env", "CACHE_BUCKET", bucketName, "SECRET_NAME", secretName)
		os.Exit(1)
	}

	ctx := context.Background()
	c, err := storage.NewClient(ctx)
	if err != nil {
		slog.Error("storage.NewClient failed", "err", err)
		os.Exit(1)
	}
	defer c.Close()
	gcsClient = c

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	http.HandleFunc("/scrape", handleScrape)
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

func scrape(ctx context.Context) error {
	token, err := getToken(ctx)
	if err != nil {
		return fmt.Errorf("get token: %w", err)
	}

	body, err := fetchFeed(ctx, fmt.Sprintf(feedURLTemplate, token))
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

func getToken(ctx context.Context) (string, error) {
	secretOnce.Do(func() {
		c, err := secretmanager.NewClient(ctx)
		if err != nil {
			secretErr = err
			return
		}
		defer c.Close()
		resp, err := c.AccessSecretVersion(ctx, &secretmanagerpb.AccessSecretVersionRequest{Name: secretName})
		if err != nil {
			secretErr = fmt.Errorf("access %s: %w", secretName, err)
			return
		}
		secretValue = string(resp.Payload.Data)
	})
	return secretValue, secretErr
}

func fetchFeed(ctx context.Context, url string) ([]byte, error) {
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
