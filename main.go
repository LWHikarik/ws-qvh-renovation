package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strings"
)

type detailsEntry struct {
	Udid           string
	ProductName    string
	ProductType    string
	ProductVersion string
}

func main() {
	log.SetLevel(log.DebugLevel)
	addr := "127.0.0.1:8080"
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}
	startWebSocketServer(addr)
}

func startWebSocketServer(addr string) {
	log.Println("Starting WebSocket server")
	stopSignal := make(chan interface{})
	stopHub := make(chan interface{})
	shutdown := make(chan interface{})
	waitForSigInt(stopSignal)
	hub := newHub()
	go hub.run(stopHub)

	m := http.NewServeMux()
	s := http.Server{Addr: addr, Handler: m}

	m.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serveWs(hub, w, r)
	})
	go func() {
		err := s.ListenAndServe()
		if err != nil {
			log.Info("s.ListenAndServe(): ", err)
		}
		stopHub <- nil
		<-stopHub
		shutdown <- nil
	}()

	<-stopSignal
	err := s.Shutdown(context.TODO())
	if err != nil {
		log.Error(err)
	} else {
		log.Info("No error on shutdown")
	}
	<-shutdown
	log.Info("Program finished")
}

func screenCaptureDevices() []byte {
	out, err := exec.Command(iosBinary(), "list").Output()
	if err != nil {
		return toErrJSON(err, "Error listing iOS devices via go-ios")
	}
	// `ios list` prints {"deviceList":[...]} on stdout; tolerate stray log lines.
	var listResp struct {
		DeviceList []string `json:"deviceList"`
	}
	for _, line := range bytes.Split(out, []byte("\n")) {
		if bytes.Contains(line, []byte("deviceList")) && json.Unmarshal(line, &listResp) == nil {
			break
		}
	}
	result := make([]detailsEntry, 0, len(listResp.DeviceList))
	for _, udid := range listResp.DeviceList {
		result = append(result, detailsEntry{Udid: udid})
	}
	text, err := json.Marshal(result)
	if err != nil {
		log.Fatalf("Broken json serialization, error: %s", err)
	}
	return text
}

// activate is a no-op for DeviceKit: there is no hidden QuickTime config to enable
// (capture happens on-device). Kept for websocket "activate" command compatibility.
func activate(udid string) []byte {
	return toJSON(map[string]interface{}{
		"device_activated": map[string]string{"udid": udid},
	})
}

func formatUdid(udid string) (string, error) {
	if len(udid) == 40 {
		return udid, nil
	}
	if len(udid) == 25 {
		return strings.Replace(udid, "-", "", 1), nil
	}
	return udid, fmt.Errorf("Invalid udid: %s", udid)
}

func waitForSigInt(stopSignalChannel chan interface{}) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for sig := range c {
			log.Debugf("Signal received: %s", sig)
			var stopSignal interface{}
			stopSignalChannel <- stopSignal
		}
	}()
}

func toErrJSON(err error, msg string) []byte {
	log.Debug(msg, err)
	return toJSON(map[string]interface{}{
		"original_error": err.Error(),
		"error_message":  msg,
	})
}

func toJSON(output interface{}) []byte {
	text, err := json.Marshal(output)
	if err != nil {
		log.Fatalf("Broken json serialization, error: %s", err)
	}
	return text
}
