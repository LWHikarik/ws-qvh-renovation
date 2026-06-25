package main

import (
	"bufio"
	"context"
	"io"
	"os"
	"os/exec"

	log "github.com/sirupsen/logrus"
)

// iosBinary is the go-ios CLI used to talk to DeviceKit. Override with WS_QVH_IOS_BIN.
func iosBinary() string {
	if b := os.Getenv("WS_QVH_IOS_BIN"); b != "" {
		return b
	}
	return "ios"
}

// startDeviceKitStream spawns `ios ui stream h264 --driver=devicekit --udid <udid>`,
// reads the Annex-B H.264 it writes to stdout, splits it into NAL units and forwards
// each one to the receiver (which prepends a 00 00 00 01 start code and fans it out to
// the connected websocket clients). The DeviceKit agent must already be running on the
// device and reachable (go-ios tunnel + `ios ui run devicekit`).
//
// This replaces the former QuickTime-over-USB (qvh) source. The websocket contract is
// unchanged: one Annex-B NAL unit per binary message, so ws-scrcpy and the browser
// player need no changes.
func startDeviceKitStream(r *ReceiverHub) {
	udid := r.udid
	r.writer = NewNaluWriter(r)

	ctx, cancel := context.WithCancel(context.Background())
	args := []string{"ui", "stream", "h264", "--driver=devicekit", "--udid", udid}
	if u := os.Getenv("WS_QVH_DEVICEKIT_URL"); u != "" {
		args = append(args, "--devicekit-url", u)
	}
	cmd := exec.CommandContext(ctx, iosBinary(), args...)
	cmd.Stderr = os.Stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		r.send <- toErrJSON(err, "devicekit: stdout pipe failed")
		return
	}
	if err := cmd.Start(); err != nil {
		cancel()
		r.send <- toErrJSON(err, "devicekit: failed to start 'ios ui stream h264'")
		return
	}
	log.Infof("DeviceKit stream started for %s", udid)

	// Stop the subprocess when the hub signals the stream to stop.
	go func() {
		<-r.stopReading
		cancel()
	}()

	go func() {
		defer cancel()
		if err := splitAnnexB(stdout, r.writer); err != nil {
			log.Error("devicekit: stream read error: ", err)
		}
		_ = cmd.Wait()
		r.writer.Stop()
		log.Infof("DeviceKit stream ended for %s", udid)
	}()
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
