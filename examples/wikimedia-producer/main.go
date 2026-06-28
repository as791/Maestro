// Command wikimedia-producer streams the public Wikimedia EventStreams
// "recentchange" Server-Sent-Events firehose and publishes each event to Kafka.
// It needs no credentials, which makes it a convenient continuous source for
// exercising the Maestro sample Flink job end-to-end.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

const defaultStreamURL = "https://stream.wikimedia.org/v2/stream/recentchange"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	brokers := strings.Split(env("KAFKA_BROKERS", "localhost:9092"), ",")
	topic := env("KAFKA_TOPIC", "wikimedia.recentchange")
	streamURL := env("STREAM_URL", defaultStreamURL)
	// Wikimedia EventStreams requires a descriptive User-Agent; without one the
	// connection succeeds (HTTP 200) but no events are delivered.
	userAgent := env("USER_AGENT", "maestro-wikimedia-producer/1.0 (https://github.com/as791/Flink-Actor-Control-Plane)")

	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		BatchTimeout: 200 * time.Millisecond,
		Async:        false,
	}
	defer writer.Close()

	slog.Info("wikimedia producer starting", "brokers", brokers, "topic", topic, "stream", streamURL)

	backoff := time.Second
	for ctx.Err() == nil {
		if err := stream(ctx, streamURL, userAgent, writer); err != nil && ctx.Err() == nil {
			slog.Warn("stream ended, reconnecting", "error", err, "backoff", backoff)
			sleep(ctx, backoff)
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = time.Second
	}
	slog.Info("wikimedia producer stopped")
}

// recentChange captures the fields we route/aggregate on; the full event is
// forwarded as the message value.
type recentChange struct {
	Wiki       string `json:"wiki"`
	ServerName string `json:"server_name"`
	Type       string `json:"type"`
}

func stream(ctx context.Context, url, userAgent string, writer *kafka.Writer) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("User-Agent", userAgent)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("stream returned HTTP %d", resp.StatusCode)
	}
	slog.Info("connected to event stream", "status", resp.StatusCode)

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	var published int
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if payload == "" {
			continue
		}
		var event recentChange
		if err := json.Unmarshal([]byte(payload), &event); err != nil {
			continue
		}
		key := event.Wiki
		if key == "" {
			key = event.ServerName
		}
		if err := writer.WriteMessages(ctx, kafka.Message{Key: []byte(key), Value: []byte(payload)}); err != nil {
			return err
		}
		published++
		if published%500 == 0 {
			slog.Info("published events", "count", published)
		}
	}
	return scanner.Err()
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
