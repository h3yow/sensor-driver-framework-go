package frame

import (
	"testing"
)

func collectHandler() (*Decoder, *[]*Frame, *[]*ParseError) {
	var good []*Frame
	var bad []*ParseError
	d := NewDecoder(func(f *Frame, e *ParseError) {
		if f != nil {
			good = append(good, f)
		} else {
			bad = append(bad, e)
		}
	})
	return d, &good, &bad
}

func mustEncode(t *testing.T, f *Frame) []byte {
	t.Helper()
	b, err := Encode(f)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return b
}

func TestRoundTrip(t *testing.T) {
	d, good, bad := collectHandler()
	in := &Frame{Type: TypeReading, Seq: 7, TS: 123456, Payload: []byte{1, 2, 3, 4}}
	d.Feed(mustEncode(t, in))
	if len(*good) != 1 || len(*bad) != 0 {
		t.Fatalf("want 1 good 0 bad, got %d/%d", len(*good), len(*bad))
	}
	got := (*good)[0]
	if got.Type != in.Type || got.Seq != in.Seq || got.TS != in.TS ||
		string(got.Payload) != string(in.Payload) {
		t.Fatalf("round trip mismatch: %+v", got)
	}
}

func TestHalfPacket(t *testing.T) {
	d, good, bad := collectHandler()
	raw := mustEncode(t, &Frame{Type: TypeReading, Seq: 1, TS: 10, Payload: []byte("abcd")})

	// 按字节喂入：在最后一个字节到来前不得成帧。
	for i := 0; i < len(raw)-1; i++ {
		d.Feed(raw[i : i+1])
		if len(*good) != 0 {
			t.Fatalf("frame completed early at byte %d", i)
		}
	}
	if d.Pending() != len(raw)-1 {
		t.Fatalf("pending=%d want %d", d.Pending(), len(raw)-1)
	}
	d.Feed(raw[len(raw)-1:])
	if len(*good) != 1 || len(*bad) != 0 {
		t.Fatalf("half packet result good=%d bad=%d", len(*good), len(*bad))
	}
}

func TestStickyPackets(t *testing.T) {
	d, good, bad := collectHandler()
	a := mustEncode(t, &Frame{Type: TypeReading, Seq: 1, TS: 1, Payload: []byte("aa")})
	b := mustEncode(t, &Frame{Type: TypeAck, Seq: 2, TS: 2, Payload: nil})
	c := mustEncode(t, &Frame{Type: TypeEvent, Seq: 3, TS: 3, Payload: make([]byte, 10)})
	d.Feed(append(append(a, b...), c...))
	if len(*good) != 3 || len(*bad) != 0 {
		t.Fatalf("want 3 good frames, got %d good %d bad", len(*good), len(*bad))
	}
	if (*good)[0].Seq != 1 || (*good)[1].Seq != 2 || (*good)[2].Seq != 3 {
		t.Fatalf("frame order/seq wrong")
	}
}

func TestBadFrameDoesNotPolluteNext(t *testing.T) {
	d, good, bad := collectHandler()
	a := mustEncode(t, &Frame{Type: TypeReading, Seq: 1, TS: 1, Payload: []byte("aa")})
	b := mustEncode(t, &Frame{Type: TypeReading, Seq: 2, TS: 2, Payload: []byte("bb")})

	// 场景1：垃圾字节 + 好帧 -> 跳过垃圾字节后好帧必须被正确解析。
	garbage := []byte{0x00, 0x11, 0xFF}
	d.Feed(append(garbage, b...))
	if len(*bad) != len(garbage) {
		t.Fatalf("want %d bad-magic events, got %d", len(garbage), len(*bad))
	}
	for _, e := range *bad {
		if e.Kind != ErrBadMagic {
			t.Fatalf("want bad_magic, got %s", e.Kind)
		}
	}
	if len(*good) != 1 || (*good)[0].Seq != 2 {
		t.Fatalf("good frame after garbage not recovered, good=%d", len(*good))
	}

	// 场景2：CRC 损坏帧 + 好帧 -> 坏帧拒绝，后续帧不受影响。
	d2, good2, bad2 := collectHandler()
	corrupt := append([]byte(nil), a...)
	corrupt[len(corrupt)-1] ^= 0xFF
	d2.Feed(append(corrupt, b...))
	if len(*bad2) != 1 || (*bad2)[0].Kind != ErrCRC {
		t.Fatalf("want 1 crc error, got %+v", *bad2)
	}
	if len(*good2) != 1 || (*good2)[0].Seq != 2 {
		t.Fatalf("frame after crc-bad frame must parse, got %d", len(*good2))
	}
}

func TestLengthTooBig(t *testing.T) {
	d, good, bad := collectHandler()
	a := mustEncode(t, &Frame{Type: TypeReading, Seq: 1, TS: 1, Payload: []byte("aa")})
	b := mustEncode(t, &Frame{Type: TypeReading, Seq: 2, TS: 2, Payload: []byte("bb")})
	// 把 a 的长度字段改成越界值，其后紧跟一帧好帧。
	a[13] = 0xFF
	a[14] = 0xFF
	d.Feed(append(a, b...))
	if len(*bad) != 1 || (*bad)[0].Kind != ErrLengthTooBig {
		t.Fatalf("want length_too_big, got %+v", *bad)
	}
	if len(*good) != 1 || (*good)[0].Seq != 2 {
		t.Fatalf("resync after bad length failed, good=%d", len(*good))
	}
}

func TestTruncatedFrameThenRest(t *testing.T) {
	d, good, _ := collectHandler()
	a := mustEncode(t, &Frame{Type: TypeReading, Seq: 1, TS: 1, Payload: make([]byte, 20)})
	// 帧体被截断：先喂整帧的前 60%，应静默等待。
	d.Feed(a[:len(a)-8])
	if len(*good) != 0 {
		t.Fatal("truncated frame must not be emitted")
	}
	d.Feed(a[len(a)-8:])
	if len(*good) != 1 {
		t.Fatal("frame should complete after remainder")
	}
}

func TestEncodeValidation(t *testing.T) {
	if _, err := Encode(&Frame{Type: Type(0x7F), Payload: nil}); err == nil {
		t.Fatal("unknown type must error")
	}
	if _, err := Encode(&Frame{Type: TypeReading, Payload: make([]byte, MaxPayloadLen+1)}); err == nil {
		t.Fatal("oversized payload must error")
	}
}
