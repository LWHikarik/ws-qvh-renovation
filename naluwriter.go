package main

var startCode = []byte{00, 00, 00, 01}

type NaluWriter struct {
	receiver *ReceiverHub
}

func NewNaluWriter(cliend *ReceiverHub) *NaluWriter {
	return &NaluWriter{receiver: cliend}
}

// writeNalu prepends an Annex-B start code to a single NAL unit and hands it to the
// receiver as one websocket binary message. append always reallocates here (startCode
// has no spare capacity), so the caller's buffer is never aliased.
func (nw NaluWriter) writeNalu(bytes []byte) error {
	if nw.receiver.closed {
		return nil
	}
	if len(bytes) > 0 {
		nw.receiver.send <- append(startCode, bytes...)
	}
	return nil
}

func (nw NaluWriter) Stop() {
}
