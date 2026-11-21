// Copyright 2026 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raftpb

// Differential tests of noax.go against protobuf-go: the clones against proto.Clone, EntrySize
// against proto.Size, the encoders against deterministic proto.Marshal, the decoders against
// proto.Unmarshal, on randomized messages covering every presence combination, nil and empty
// slices, unknown fields and enum values out of range.

import (
	"bytes"
	"math"
	"math/rand"
	"reflect"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

const iters = 20000

// strictEqual reports a difference between a and b: values, nil-ness of every pointer and
// slice, and the unknown fields (the message state and size cache are ignored).
func strictEqual(t *testing.T, path string, a, b reflect.Value) bool {
	t.Helper()
	if a.Kind() != b.Kind() {
		t.Errorf("%s: kinds %v and %v", path, a.Kind(), b.Kind())
		return false
	}
	switch a.Kind() {
	case reflect.Pointer:
		if a.IsNil() != b.IsNil() {
			t.Errorf("%s: nil %v vs %v", path, a.IsNil(), b.IsNil())
			return false
		}
		if a.IsNil() {
			return true
		}
		return strictEqual(t, path, a.Elem(), b.Elem())
	case reflect.Slice:
		if a.IsNil() != b.IsNil() {
			t.Errorf("%s: nil %v vs %v", path, a.IsNil(), b.IsNil())
			return false
		}
		if a.Len() != b.Len() {
			t.Errorf("%s: len %d vs %d", path, a.Len(), b.Len())
			return false
		}
		for i := 0; i < a.Len(); i++ {
			if !strictEqual(t, path+"[]", a.Index(i), b.Index(i)) {
				return false
			}
		}
		return true
	case reflect.Struct:
		for i := 0; i < a.NumField(); i++ {
			name := a.Type().Field(i).Name
			if name == "state" || name == "sizeCache" {
				continue
			}
			if !strictEqual(t, path+"."+name, a.Field(i), b.Field(i)) {
				return false
			}
		}
		return true
	case reflect.Uint64, reflect.Uint8:
		if a.Uint() != b.Uint() {
			t.Errorf("%s: %d vs %d", path, a.Uint(), b.Uint())
			return false
		}
		return true
	case reflect.Int32:
		if a.Int() != b.Int() {
			t.Errorf("%s: %d vs %d", path, a.Int(), b.Int())
			return false
		}
		return true
	case reflect.Bool:
		if a.Bool() != b.Bool() {
			t.Errorf("%s: %v vs %v", path, a.Bool(), b.Bool())
			return false
		}
		return true
	}
	t.Fatalf("%s: unexpected kind %v", path, a.Kind())
	return false
}

// noAlias reports a pointer or a non-empty slice of the clone c shared with the original o.
func noAlias(t *testing.T, path string, c, o reflect.Value) bool {
	t.Helper()
	switch c.Kind() {
	case reflect.Pointer:
		if c.IsNil() || o.IsNil() {
			return true
		}
		if c.Pointer() == o.Pointer() {
			t.Errorf("%s: pointer shared", path)
			return false
		}
		return noAlias(t, path, c.Elem(), o.Elem())
	case reflect.Slice:
		if c.Len() == 0 || o.Len() == 0 {
			return true
		}
		if c.Pointer() == o.Pointer() {
			t.Errorf("%s: slice shared", path)
			return false
		}
		for i := 0; i < c.Len() && i < o.Len(); i++ {
			if !noAlias(t, path+"[]", c.Index(i), o.Index(i)) {
				return false
			}
		}
	case reflect.Struct:
		for i := 0; i < c.NumField(); i++ {
			name := c.Type().Field(i).Name
			if name == "state" || name == "sizeCache" {
				continue
			}
			if !noAlias(t, path+"."+name, c.Field(i), o.Field(i)) {
				return false
			}
		}
	}
	return true
}

func same(t *testing.T, what string, a, b any) bool {
	t.Helper()
	return strictEqual(t, what, reflect.ValueOf(a), reflect.ValueOf(b))
}

type gen struct{ r *rand.Rand }

func (g gen) u64() uint64 {
	switch g.r.Intn(6) {
	case 0:
		return 0
	case 1:
		return uint64(g.r.Intn(300))
	case 2:
		return []uint64{127, 128, 16383, 16384, 1<<63 - 1, 1 << 63, math.MaxUint64}[g.r.Intn(7)]
	case 3:
		return g.r.Uint64() >> uint(g.r.Intn(64))
	default:
		return g.r.Uint64()
	}
}

func (g gen) optU64() *uint64 {
	if g.r.Intn(3) == 0 {
		return nil
	}
	return new(g.u64())
}

func (g gen) optBool() *bool {
	if g.r.Intn(3) == 0 {
		return nil
	}
	return new(g.r.Intn(2) == 0)
}

// enum returns an enum value: mostly one of the n defined ones, sometimes any int32.
func (g gen) enum(n int) int32 {
	switch g.r.Intn(8) {
	case 0:
		return []int32{-1, math.MinInt32, math.MaxInt32, int32(n), 1000}[g.r.Intn(5)]
	case 1:
		return int32(g.r.Uint32())
	default:
		return int32(g.r.Intn(n))
	}
}

func (g gen) bytes() []byte {
	switch g.r.Intn(4) {
	case 0:
		return nil
	case 1:
		return []byte{}
	default:
		b := make([]byte, g.r.Intn(200))
		g.r.Read(b)
		return b
	}
}

func (g gen) u64s() []uint64 {
	switch g.r.Intn(4) {
	case 0:
		return nil
	case 1:
		return []uint64{}
	default:
		s := make([]uint64, g.r.Intn(8))
		for i := range s {
			s[i] = g.u64()
		}
		return s
	}
}

// unknown returns unknown fields: none (nil or empty), arbitrary bytes, or well-formed fields
// of field numbers the messages do not use.
func (g gen) unknown() []byte {
	switch g.r.Intn(4) {
	case 0:
		return nil
	case 1:
		return []byte{}
	case 2:
		return g.bytes()
	default:
		var b []byte
		for range g.r.Intn(3) + 1 {
			b = append(b, byte(20+g.r.Intn(10))<<3) // varint field 20..29
			b = appendVarint(b, g.u64())
		}
		return b
	}
}

func (g gen) entry() *Entry {
	e := &Entry{Term: g.optU64(), Index: g.optU64(), Data: g.bytes(), unknownFields: g.unknown()}
	if g.r.Intn(3) != 0 {
		e.Type = new(EntryType(g.enum(3)))
	}
	return e
}

func (g gen) confState() *ConfState {
	return &ConfState{Voters: g.u64s(), Learners: g.u64s(), VotersOutgoing: g.u64s(),
		LearnersNext: g.u64s(), AutoLeave: g.optBool(), unknownFields: g.unknown()}
}

func (g gen) snapshot() *Snapshot {
	s := &Snapshot{Data: g.bytes(), unknownFields: g.unknown()}
	if g.r.Intn(4) != 0 {
		s.Metadata = &SnapshotMetadata{Index: g.optU64(), Term: g.optU64(), unknownFields: g.unknown()}
		if g.r.Intn(4) != 0 {
			s.Metadata.ConfState = g.confState()
		}
	}
	return s
}

// confChange returns a ConfChange with unknown fields only if withUnknown.
func (g gen) confChange(withUnknown bool) *ConfChange {
	c := &ConfChange{Id: g.optU64(), NodeId: g.optU64(), Context: g.bytes()}
	if g.r.Intn(3) != 0 {
		c.Type = new(ConfChangeType(g.enum(4)))
	}
	if withUnknown {
		c.unknownFields = g.unknown()
	}
	return c
}

// confChangeV2 returns a ConfChangeV2 with unknown fields only if withUnknown, and nil
// changes only if withNil.
func (g gen) confChangeV2(withUnknown, withNil bool) *ConfChangeV2 {
	c := &ConfChangeV2{Context: g.bytes()}
	if g.r.Intn(3) != 0 {
		c.Transition = new(ConfChangeTransition(g.enum(3)))
	}
	switch g.r.Intn(4) {
	case 0:
	case 1:
		c.Changes = []*ConfChangeSingle{}
	default:
		for range g.r.Intn(5) {
			if withNil && g.r.Intn(5) == 0 {
				c.Changes = append(c.Changes, nil)
				continue
			}
			s := &ConfChangeSingle{NodeId: g.optU64()}
			if g.r.Intn(3) != 0 {
				s.Type = new(ConfChangeType(g.enum(4)))
			}
			if withUnknown {
				s.unknownFields = g.unknown()
			}
			c.Changes = append(c.Changes, s)
		}
	}
	if withUnknown {
		c.unknownFields = g.unknown()
	}
	return c
}

func newGen(t *testing.T) gen {
	seed := rand.Int63()
	t.Logf("seed %d", seed)
	return gen{rand.New(rand.NewSource(seed))}
}

func TestCloneMatchesProtoClone(t *testing.T) {
	g := newGen(t)
	for range iters {
		e := g.entry()
		c, p := CloneEntry(e), proto.Clone(e).(*Entry)
		if !same(t, "Entry", c, p) || !noAlias(t, "Entry", reflect.ValueOf(c), reflect.ValueOf(e)) ||
			!proto.Equal(c, e) {
			t.Fatalf("CloneEntry(%v)", e)
		}
		cs := g.confState()
		ccs, pcs := CloneConfState(cs), proto.Clone(cs).(*ConfState)
		if !same(t, "ConfState", ccs, pcs) || !noAlias(t, "ConfState", reflect.ValueOf(ccs), reflect.ValueOf(cs)) ||
			!proto.Equal(ccs, cs) {
			t.Fatalf("CloneConfState(%v)", cs)
		}
		s := g.snapshot()
		cs2, ps := CloneSnapshot(s), proto.Clone(s).(*Snapshot)
		if !same(t, "Snapshot", cs2, ps) || !noAlias(t, "Snapshot", reflect.ValueOf(cs2), reflect.ValueOf(s)) ||
			!proto.Equal(cs2, s) {
			t.Fatalf("CloneSnapshot(%v)", s)
		}
	}
	// A nil message clones to a typed nil, as proto.Clone.
	if CloneEntry(nil) != nil || proto.Clone((*Entry)(nil)).(*Entry) != nil {
		t.Fatal("CloneEntry(nil)")
	}
	if CloneConfState(nil) != nil || proto.Clone((*ConfState)(nil)).(*ConfState) != nil {
		t.Fatal("CloneConfState(nil)")
	}
	if CloneSnapshot(nil) != nil || proto.Clone((*Snapshot)(nil)).(*Snapshot) != nil {
		t.Fatal("CloneSnapshot(nil)")
	}
}

func TestEntrySizeMatchesProtoSize(t *testing.T) {
	g := newGen(t)
	for range iters {
		e := g.entry()
		if got, want := EntrySize(e), proto.Size(e); got != want {
			t.Fatalf("EntrySize(%v) = %d, proto.Size = %d", e, got, want)
		}
	}
	if EntrySize(nil) != proto.Size((*Entry)(nil)) {
		t.Fatal("EntrySize(nil)")
	}
}

var det = proto.MarshalOptions{Deterministic: true}

func TestMarshalMatchesProtoMarshal(t *testing.T) {
	g := newGen(t)
	for range iters {
		c := g.confChange(false)
		want, err := det.Marshal(c)
		if err != nil {
			t.Fatal(err)
		}
		if got := MarshalConfChangeV1(c); !bytes.Equal(got, want) || (got == nil) != (want == nil) {
			t.Fatalf("MarshalConfChangeV1(%v) = %x, proto.Marshal = %x", c, got, want)
		}
		c2 := g.confChangeV2(false, true)
		want, err = det.Marshal(c2)
		if err != nil {
			t.Fatal(err)
		}
		if got := MarshalConfChangeV2(c2); !bytes.Equal(got, want) || (got == nil) != (want == nil) {
			t.Fatalf("MarshalConfChangeV2(%v) = %x, proto.Marshal = %x", c2, got, want)
		}
	}
	// A nil message encodes to nil, as proto.Marshal.
	if b, _ := proto.Marshal((*ConfChange)(nil)); MarshalConfChangeV1(nil) != nil || b != nil {
		t.Fatal("MarshalConfChangeV1(nil)")
	}
	if b, _ := proto.Marshal((*ConfChangeV2)(nil)); MarshalConfChangeV2(nil) != nil || b != nil {
		t.Fatal("MarshalConfChangeV2(nil)")
	}
}

// normV2 is what decoding c's encoding gives: no changes as nil, a nil change as an empty one.
func normV2(c *ConfChangeV2) *ConfChangeV2 {
	n := &ConfChangeV2{Transition: c.Transition, Context: c.Context}
	for _, s := range c.Changes {
		if s == nil {
			s = &ConfChangeSingle{}
		}
		n.Changes = append(n.Changes, &ConfChangeSingle{Type: s.Type, NodeId: s.NodeId})
	}
	return n
}

func TestUnmarshalRoundTrip(t *testing.T) {
	g := newGen(t)
	for range iters {
		c := g.confChange(false)
		b := MarshalConfChangeV1(c)
		var d, p ConfChange
		if err := UnmarshalConfChangeV1(b, &d); err != nil {
			t.Fatalf("UnmarshalConfChangeV1(%x): %v", b, err)
		}
		if !same(t, "ConfChange", &d, c) {
			t.Fatalf("UnmarshalConfChangeV1(MarshalConfChangeV1(%v)) = %v", c, &d)
		}
		if err := proto.Unmarshal(b, &p); err != nil {
			t.Fatal(err)
		}
		if !same(t, "ConfChange vs proto", &d, &p) {
			t.Fatalf("UnmarshalConfChangeV1(%x) = %v, proto.Unmarshal = %v", b, &d, &p)
		}

		c2 := g.confChangeV2(false, true)
		b = MarshalConfChangeV2(c2)
		var d2, p2 ConfChangeV2
		if err := UnmarshalConfChangeV2(b, &d2); err != nil {
			t.Fatalf("UnmarshalConfChangeV2(%x): %v", b, err)
		}
		if !same(t, "ConfChangeV2", &d2, normV2(c2)) {
			t.Fatalf("UnmarshalConfChangeV2(MarshalConfChangeV2(%v)) = %v", c2, &d2)
		}
		if err := proto.Unmarshal(b, &p2); err != nil {
			t.Fatal(err)
		}
		if !same(t, "ConfChangeV2 vs proto", &d2, &p2) {
			t.Fatalf("UnmarshalConfChangeV2(%x) = %v, proto.Unmarshal = %v", b, &d2, &p2)
		}
	}
	// The empty input decodes to the empty message, as with proto.Unmarshal.
	for _, b := range [][]byte{nil, {}} {
		var d ConfChange
		if err := UnmarshalConfChangeV1(b, &d); err != nil || !same(t, "empty", &d, &ConfChange{}) {
			t.Fatalf("UnmarshalConfChangeV1(%v) = %v, %v", b, &d, err)
		}
		var d2 ConfChangeV2
		if err := UnmarshalConfChangeV2(b, &d2); err != nil || !same(t, "empty", &d2, &ConfChangeV2{}) {
			t.Fatalf("UnmarshalConfChangeV2(%v) = %v, %v", b, &d2, err)
		}
	}
	// The decoder resets the message.
	d := ConfChangeV2{Transition: ConfChangeTransitionJointExplicit.Enum(), Context: []byte("x"),
		Changes: []*ConfChangeSingle{{}}}
	if err := UnmarshalConfChangeV2(nil, &d); err != nil || !same(t, "reset", &d, &ConfChangeV2{}) {
		t.Fatalf("UnmarshalConfChangeV2(nil) into a set message = %v, %v", &d, err)
	}
}

// checkDecode compares acceptance, decoded fields and the subsequent serialization.
func checkDecode(t *testing.T, b []byte) (bool, bool) {
	t.Helper()
	check := func(want, got proto.Message, err error, marshal func() []byte) bool {
		t.Helper()
		refErr := proto.Unmarshal(b, want)
		if (err == nil) != (refErr == nil) {
			t.Fatalf("%T decoding %x: got %v, protobuf-go %v", got, b, err, refErr)
		}
		if err != nil {
			return false
		}
		if !same(t, "decoded", got, want) {
			t.Fatalf("%T decoding %x differs from protobuf-go", got, b)
		}
		wire, refErr := det.Marshal(want)
		if refErr != nil || !bytes.Equal(marshal(), wire) {
			t.Fatalf("%T round trip of %x differs from protobuf-go", got, b)
		}
		return true
	}
	var v1 ConfChange
	var v2 ConfChangeV2
	return check(&ConfChange{}, &v1, UnmarshalConfChangeV1(b, &v1), func() []byte { return MarshalConfChangeV1(&v1) }),
		check(&ConfChangeV2{}, &v2, UnmarshalConfChangeV2(b, &v2), func() []byte { return MarshalConfChangeV2(&v2) })
}

func TestUnmarshalWireCompatibility(t *testing.T) {
	g := newGen(t)
	accepted, rejected := 0, 0
	for range iters {
		var b []byte
		switch g.r.Intn(4) {
		case 0: // garbage
			b = g.bytes()
		case 1: // an encoding with unknown fields
			b, _ = det.Marshal(g.confChangeV2(true, false))
		case 2: // an encoding with unknown fields
			b, _ = det.Marshal(g.confChange(true))
		default: // a mutated encoding
			if g.r.Intn(2) == 0 {
				b = MarshalConfChangeV1(g.confChange(false))
			} else {
				b = MarshalConfChangeV2(g.confChangeV2(false, false))
			}
			b = append([]byte{}, b...)
			for range g.r.Intn(3) + 1 {
				switch {
				case len(b) == 0:
					b = append(b, byte(g.r.Intn(256)))
				case g.r.Intn(3) == 0:
					b = b[:g.r.Intn(len(b))]
				case g.r.Intn(2) == 0:
					b[g.r.Intn(len(b))] = byte(g.r.Intn(256))
				default:
					i := g.r.Intn(len(b) + 1)
					b = append(b[:i], append([]byte{byte(g.r.Intn(256))}, b[i:]...)...)
				}
			}
		}
		ok1, ok2 := checkDecode(t, b)
		if ok1 || ok2 {
			accepted++
		} else {
			rejected++
		}
	}
	t.Logf("accepted %d, rejected %d", accepted, rejected)
	// Compare valid alternative encodings and malformed inputs against protobuf-go.
	for _, b := range [][]byte{
		{0x10, 0x01, 0x08, 0x01}, // V1: type before id
		{0x08, 0x01, 0x08, 0x01}, // V1: id twice
		{0x08, 0x80, 0x00},       // non-shortest varint
		{0x08, 0x81, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02}, // varint over 64 bits
		{0x10, 0x80, 0x80, 0x80, 0x80, 0x08},                               // enum 2^31
		{0x22, 0x05, 0x01},                                                 // V1: context truncated
		{0x08},                                                             // truncated varint
		{0xa0, 0x01, 0x01},                                                 // field 20
		{0x12, 0x02, 0x10, 0x01, 0x08, 0x01},                               // V2: a transition after a change
		{0x1a, 0x00, 0x12, 0x00},                                           // V2: a change after the context
		{0x12, 0x04, 0x10, 0x01, 0x08, 0x01},                               // V2: a change with node_id before type
	} {
		checkDecode(t, b)
	}
	// A negative enum is a 10-byte varint, accepted (and agreeing with proto.Unmarshal, which
	// keeps an out-of-range value of a closed enum in the field, as the round trip tests check).
	b := MarshalConfChangeV2(&ConfChangeV2{Transition: new(ConfChangeTransition(-1))})
	if len(b) != 11 {
		t.Fatalf("encoding of transition -1: %x", b)
	}
	if _, ok2 := checkDecode(t, b); !ok2 {
		t.Fatalf("UnmarshalConfChangeV2 rejects %x", b)
	}
}

// The message decoder and protowire have different field-number limits
// inside groups. Compare the complete retained bytes with protobuf-go.
func TestUnknownConfChangeGroup(t *testing.T) {
	for _, inner := range []protowire.Number{21, 1 << 29, 1<<31 - 1} {
		wire := protowire.AppendTag(nil, 20, protowire.StartGroupType)
		wire = protowire.AppendTag(wire, inner, protowire.Fixed64Type)
		wire = protowire.AppendFixed64(wire, 123)
		wire = protowire.AppendTag(wire, 20, protowire.EndGroupType)
		checkDecode(t, wire)
	}
}

func FuzzUnmarshalConfChange(f *testing.F) {
	f.Add([]byte{})
	f.Add(MarshalConfChangeV1(&ConfChange{Id: new(uint64(1)), Type: ConfChangeAddNode.Enum(),
		NodeId: new(uint64(300)), Context: []byte("ctx")}))
	f.Add(MarshalConfChangeV2(&ConfChangeV2{Transition: ConfChangeTransitionJointExplicit.Enum(),
		Changes: []*ConfChangeSingle{{Type: ConfChangeAddLearnerNode.Enum(), NodeId: new(uint64(2))}, {}},
		Context: []byte{}}))
	f.Fuzz(func(t *testing.T, b []byte) {
		checkDecode(t, b)
	})
}
