package frame

import (
	"bytes"
	"testing"
)

func mkFrame(seq uint16, payload string) Frame {
	return Frame{Type: 0x01, Seq: seq, Payload: []byte(payload)}
}

func TestRoundTrip(t *testing.T) {
	d := NewDecoder(3)
	frames := d.Feed(Encode(mkFrame(1, "hello")))
	if len(frames) != 1 || string(frames[0].Payload) != "hello" || frames[0].Seq != 1 {
		t.Fatalf("round trip failed: %+v", frames)
	}
}

func TestHalfPacket(t *testing.T) {
	d := NewDecoder(3)
	raw := Encode(mkFrame(7, "split-me"))
	// 分两次喂入：先一半，再一半。
	if got := d.Feed(raw[:5]); len(got) != 0 {
		t.Fatalf("half packet should yield no frame, got %v", got)
	}
	got := d.Feed(raw[5:])
	if len(got) != 1 || string(got[0].Payload) != "split-me" || got[0].Seq != 7 {
		t.Fatalf("half packet reassembly failed: %+v", got)
	}
}

func TestStickyPackets(t *testing.T) {
	d := NewDecoder(3)
	var buf bytes.Buffer
	buf.Write(Encode(mkFrame(1, "a")))
	buf.Write(Encode(mkFrame(2, "bb")))
	buf.Write(Encode(mkFrame(3, "ccc")))
	got := d.Feed(buf.Bytes())
	if len(got) != 3 {
		t.Fatalf("sticky packets should yield 3 frames, got %d", len(got))
	}
	for i, want := range []string{"a", "bb", "ccc"} {
		if string(got[i].Payload) != want {
			t.Fatalf("frame %d payload = %q, want %q", i, got[i].Payload, want)
		}
	}
}

func TestBadCRCDoesNotPoisonNext(t *testing.T) {
	d := NewDecoder(3)
	bad := Encode(mkFrame(1, "corrupt"))
	bad[len(bad)-1] ^= 0xFF // 破坏 CRC
	good := Encode(mkFrame(2, "good"))
	got := d.Feed(append(bad, good...))
	if len(got) != 1 || string(got[0].Payload) != "good" {
		t.Fatalf("bad crc poisoned stream: %+v", got)
	}
	if d.Stats().BadCRC != 1 {
		t.Fatalf("BadCRC = %d, want 1", d.Stats().BadCRC)
	}
}

func TestGarbageResync(t *testing.T) {
	d := NewDecoder(3)
	data := append([]byte{0x00, 0x11, 0x22, 0xAA}, Encode(mkFrame(9, "ok"))...)
	got := d.Feed(data)
	if len(got) != 1 || string(got[0].Payload) != "ok" {
		t.Fatalf("resync failed: %+v", got)
	}
	if d.Stats().BytesDropped == 0 {
		t.Fatal("expected dropped bytes during resync")
	}
}

func TestRecoveryCallback(t *testing.T) {
	var fired int
	d := NewDecoder(2)
	d.OnRecovery = func(s Stats) { fired++ }
	for i := 0; i < 2; i++ {
		bad := Encode(mkFrame(uint16(i), "x"))
		bad[len(bad)-1] ^= 0xFF
		d.Feed(bad)
	}
	if fired != 1 {
		t.Fatalf("recovery callback fired %d times, want 1", fired)
	}
}

func TestCRC16KnownVector(t *testing.T) {
	// "123456789" 的 CRC-16/CCITT-FALSE = 0x29B1
	if got := CRC16([]byte("123456789")); got != 0x29B1 {
		t.Fatalf("CRC16 = %#x, want 0x29b1", got)
	}
}
