package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

// DeviceKit (github.com/mobile-next/devicekit-ios) is an on-device JSON-RPC +
// streaming agent. Its HTTP server is reachable on the host once go-ios forwards
// the device port (default 127.0.0.1:12004) — e.g. via `ios ui run devicekit`.
//
// We pull H.264 straight from its `/h264` endpoint with OUR OWN http client, for
// two reasons go-ios' `ios ui stream` cannot give us:
//   - No client Timeout. go-ios builds its client as http.Client{Timeout: 60s},
//     and Go's Client.Timeout includes reading the body — so it hard-caps any
//     stream at 60s ("stream copy failed: context deadline exceeded"). That was
//     the "video freezes, reload the tab" bug. We use an idle-read watchdog
//     instead, which only trips on a genuine stall.
//   - Reconnect. On a stall/EOF/transient error we reconnect with backoff rather
//     than ending the stream.
//
// The websocket contract downstream is unchanged: one Annex-B NAL unit per
// binary message, so ws-scrcpy and the browser player need no changes.

const (
	defaultDeviceKitURL = "http://127.0.0.1:12004"

	// Stream defaults favour quality over the agent's conservative defaults
	// (scale=50, fps=30, bitrate=4Mbit, quality=60). scale=100 (full res) +
	// bitrate maxed at the agent cap (10Mbit) minimise macroblocking on motion;
	// fps=45 balances smoothness against device heat — the 0.0.18 /h264 capture is
	// screenshot-based and CPU-bound by fps, so fps is the main thermal lever (drop
	// to 30 per-device for older/hot iPhones, e.g. 11/12, via WS_QVH_FPS). Agent
	// caps: fps 1-60, scale 10-100, quality 1-100, bitrate 100000-10000000.
	// Override per-deployment via the WS_QVH_* env vars below.
	defaultScale   = "100"
	defaultFPS     = "45"
	defaultBitrate = "10000000"
	defaultQuality = "90"

	// If no frame data arrives for this long the connection is considered dead
	// and we reconnect. The agent streams continuously, so a gap this large is a
	// real stall. Override with WS_QVH_STREAM_IDLE_TIMEOUT (seconds).
	defaultIdleTimeout = 10 * time.Second

	reconnectBackoffMin = 500 * time.Millisecond
	reconnectBackoffMax = 5 * time.Second
)

// iosBinary is the go-ios CLI. Override with WS_QVH_IOS_BIN (the farm's go-ios is
// named `ios-tool`, not on PATH). Still used by main.go for `ios list` device
// discovery; the video stream itself no longer shells out to it.
func iosBinary() string {
	if b := os.Getenv("WS_QVH_IOS_BIN"); b != "" {
		return b
	}
	return "ios"
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// deviceKitStreamURL builds the /h264 URL with quality params from the env.
//
// NOTE (Step 1 limitation): the base URL identifies the device — the DeviceKit
// agent on a forwarded port serves exactly one device. With a single shared
// WS_QVH_DEVICEKIT_URL this is one-device-per-host. Per-device port allocation
// (ws-qvh owning the tunnel + forward) is Step 2 (roadmap §4.1).
func deviceKitStreamURL() string {
	base := env("WS_QVH_DEVICEKIT_URL", defaultDeviceKitURL)
	q := url.Values{}
	q.Set("fps", env("WS_QVH_FPS", defaultFPS))
	q.Set("scale", env("WS_QVH_SCALE", defaultScale))
	q.Set("bitrate", env("WS_QVH_BITRATE", defaultBitrate))
	q.Set("quality", env("WS_QVH_QUALITY", defaultQuality))
	return strings.TrimRight(base, "/") + "/h264?" + q.Encode()
}

func idleTimeout() time.Duration {
	if v := os.Getenv("WS_QVH_STREAM_IDLE_TIMEOUT"); v != "" {
		if secs, err := strconv.Atoi(v); err == nil && secs > 0 {
			return time.Duration(secs) * time.Second
		}
	}
	return defaultIdleTimeout
}

// startDeviceKitStream pulls the device's H.264 from the DeviceKit agent over
// HTTP, splits the Annex-B byte stream into NAL units and forwards each one to
// the receiver (which caches SPS/PPS/SEI and fans it out to the websocket
// clients). It reconnects automatically until the hub signals the stream to stop.
func startDeviceKitStream(r *ReceiverHub) {
	r.writer = NewNaluWriter(r)

	ctx, cancel := context.WithCancel(context.Background())
	// The hub signals end-of-stream by closing/sending on stopReading.
	go func() {
		<-r.stopReading
		cancel()
	}()

	go func() {
		defer r.writer.Stop()
		streamURL := deviceKitStreamURL()
		log.Infof("DeviceKit stream starting for %s from %s", r.udid, streamURL)
		runDeviceKitStream(ctx, r, streamURL)
		log.Infof("DeviceKit stream ended for %s", r.udid)
	}()
}

// runDeviceKitStream keeps the stream alive across transient failures with
// capped exponential backoff, until ctx is cancelled (stop requested).
func runDeviceKitStream(ctx context.Context, r *ReceiverHub, streamURL string) {
	backoff := reconnectBackoffMin
	for ctx.Err() == nil {
		start := time.Now()
		err := streamOnce(ctx, streamURL, r.writer)
		if ctx.Err() != nil {
			return // stop requested — not an error
		}
		if err != nil {
			log.Warnf("devicekit: stream for %s interrupted: %v", r.udid, err)
		} else {
			log.Infof("devicekit: stream for %s ended (EOF)", r.udid)
		}
		// A connection that lasted a while was healthy — reset the backoff so a
		// single hiccup doesn't slow later reconnects.
		if time.Since(start) > 5*time.Second {
			backoff = reconnectBackoffMin
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < reconnectBackoffMax {
			backoff *= 2
			if backoff > reconnectBackoffMax {
				backoff = reconnectBackoffMax
			}
		}
	}
}

// streamOnce performs a single GET and pumps NAL units until the body ends, the
// stream stalls (no data within the idle timeout), or ctx is cancelled.
func streamOnce(ctx context.Context, streamURL string, writer *NaluWriter) error {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	req, err := http.NewRequestWithContext(streamCtx, http.MethodGet, streamURL, nil)
	if err != nil {
		return err
	}
	// Deliberately no client Timeout — that would cap the whole stream. Stalls
	// are caught by the idle watchdog below instead.
	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("devicekit returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}

	// Watchdog: cancel the request if no bytes arrive within the idle timeout.
	idle := idleTimeout()
	watchdog := time.AfterFunc(idle, cancel)
	defer watchdog.Stop()
	body := &idleResetReader{r: resp.Body, reset: func() { watchdog.Reset(idle) }}

	return splitAnnexB(body, writer)
}

// idleResetReader resets a watchdog timer whenever data is read, so a stalled
// stream (no frames) trips the timeout while an active one never does.
type idleResetReader struct {
	r     io.Reader
	reset func()
}

func (ir *idleResetReader) Read(p []byte) (int, error) {
	n, err := ir.r.Read(p)
	if n > 0 {
		ir.reset()
	}
	return n, err
}

// splitAnnexB reads an Annex-B H.264 byte stream and calls writer.writeNalu for every
// NAL unit (start code stripped). It blocks until src returns EOF or an error.
func splitAnnexB(src io.Reader, writer *NaluWriter) error {
	sc := bufio.NewScanner(src)
	sc.Buffer(make([]byte, 0, 1<<20), 8<<20) // grow up to 8 MiB per NAL unit
	sc.Split(annexBSplitFunc)
	for sc.Scan() {
		nalu := sc.Bytes()
		if len(nalu) == 0 {
			continue
		}
		// writeNalu does append(startCode, nalu...) which copies, so handing it the
		// scanner's transient buffer is safe.
		if err := writer.writeNalu(nalu); err != nil {
			return err
		}
	}
	return sc.Err()
}

// annexBSplitFunc is a bufio.SplitFunc yielding one NAL unit per token, excluding the
// start code. Handles both 00 00 01 and 00 00 00 01 start codes across buffer reads.
func annexBSplitFunc(data []byte, atEOF bool) (advance int, token []byte, err error) {
	i := indexStartCode(data, 0)
	if i < 0 {
		if atEOF {
			return len(data), nil, nil
		}
		return 0, nil, nil
	}
	payload := i + 3 // skip 00 00 01
	j := indexStartCode(data, payload)
	if j < 0 {
		if atEOF {
			return len(data), trimTrailingZero(data[payload:]), nil
		}
		return 0, nil, nil // need more data to find the NAL unit end
	}
	end := j
	if end > payload && data[end-1] == 0 {
		end-- // the extra leading zero belongs to a 4-byte start code
	}
	return j, data[payload:end], nil
}

// indexStartCode returns the index of the next 00 00 01 sequence at/after from, or -1.
func indexStartCode(data []byte, from int) int {
	for i := from; i+2 < len(data); i++ {
		if data[i] == 0 && data[i+1] == 0 && data[i+2] == 1 {
			return i
		}
	}
	return -1
}

func trimTrailingZero(b []byte) []byte {
	for len(b) > 0 && b[len(b)-1] == 0 {
		b = b[:len(b)-1]
	}
	return b
}
