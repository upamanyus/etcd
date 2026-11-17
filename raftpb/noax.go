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

// Hand-written replacements for the uses raft makes of google.golang.org/protobuf/proto
// (Clone, Size, Marshal and Unmarshal of a few message types), written with byte slices and
// integer arithmetic only (no reflect, no unsafe), so that they can be verified as ordinary
// Go code. Each is checked against protobuf-go by the differential tests of noax_test.go.

import "errors"

// cloneUint64 returns a fresh pointer to *p, or nil if p is nil.
func cloneUint64(p *uint64) *uint64 {
	if p == nil {
		return nil
	}
	return new(*p)
}

// cloneBool returns a fresh pointer to *p, or nil if p is nil.
func cloneBool(p *bool) *bool {
	if p == nil {
		return nil
	}
	return new(*p)
}

// cloneBytes clones a bytes field as proto.Clone does: nil stays nil (an unset field), any
// other slice (an empty one included) becomes a fresh non-nil slice.
func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	return append([]byte{}, b...)
}

// cloneUint64s clones a repeated scalar field as proto.Clone does (appending onto the nil
// slice): an empty slice, nil or not, becomes nil; any other a fresh slice.
func cloneUint64s(s []uint64) []uint64 {
	return append([]uint64(nil), s...)
}

// cloneUnknown clones the unknown fields as proto.Clone does: empty ones become nil, any
// others a fresh slice.
func cloneUnknown(u []byte) []byte {
	return append([]byte(nil), u...)
}

// CloneEntry returns a deep copy of e, equal to what proto.Clone(e) returns: every set field
// copied into a fresh field, and the unknown fields copied; nil if e is nil.
func CloneEntry(e *Entry) *Entry {
	if e == nil {
		return nil
	}
	c := &Entry{}
	c.Term = cloneUint64(e.Term)
	c.Index = cloneUint64(e.Index)
	if e.Type != nil {
		c.Type = new(*e.Type)
	}
	c.Data = cloneBytes(e.Data)
	c.unknownFields = cloneUnknown(e.unknownFields)
	return c
}

// CloneConfState returns a deep copy of cs, equal to what proto.Clone(cs) returns: every set
// field copied into a fresh field, and the unknown fields copied; nil if cs is nil.
func CloneConfState(cs *ConfState) *ConfState {
	if cs == nil {
		return nil
	}
	c := &ConfState{}
	c.Voters = cloneUint64s(cs.Voters)
	c.Learners = cloneUint64s(cs.Learners)
	c.VotersOutgoing = cloneUint64s(cs.VotersOutgoing)
	c.LearnersNext = cloneUint64s(cs.LearnersNext)
	c.AutoLeave = cloneBool(cs.AutoLeave)
	c.unknownFields = cloneUnknown(cs.unknownFields)
	return c
}

// cloneSnapshotMetadata returns a deep copy of m, as proto.Clone(m); nil if m is nil.
func cloneSnapshotMetadata(m *SnapshotMetadata) *SnapshotMetadata {
	if m == nil {
		return nil
	}
	c := &SnapshotMetadata{}
	c.ConfState = CloneConfState(m.ConfState)
	c.Index = cloneUint64(m.Index)
	c.Term = cloneUint64(m.Term)
	c.unknownFields = cloneUnknown(m.unknownFields)
	return c
}

// CloneSnapshot returns a deep copy of s, equal to what proto.Clone(s) returns: every set
// field copied into a fresh field (the metadata and its conf state fresh messages), and the
// unknown fields copied; nil if s is nil.
func CloneSnapshot(s *Snapshot) *Snapshot {
	if s == nil {
		return nil
	}
	c := &Snapshot{}
	c.Data = cloneBytes(s.Data)
	c.Metadata = cloneSnapshotMetadata(s.Metadata)
	c.unknownFields = cloneUnknown(s.unknownFields)
	return c
}

// sizeVarint returns the length of the varint encoding of v.
func sizeVarint(v uint64) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
	}
	return n
}

// EntrySize returns the size of e's protobuf encoding, equal to proto.Size(e): for each set
// field, its one-byte tag and its value (an enum as the varint of its sign-extended value,
// bytes as their length's varint and the bytes), and the unknown fields; 0 if e is nil.
func EntrySize(e *Entry) int {
	if e == nil {
		return 0
	}
	n := 0
	if e.Type != nil {
		n += 1 + sizeVarint(uint64(int64(*e.Type)))
	}
	if e.Term != nil {
		n += 1 + sizeVarint(*e.Term)
	}
	if e.Index != nil {
		n += 1 + sizeVarint(*e.Index)
	}
	if e.Data != nil {
		n += 1 + sizeVarint(uint64(len(e.Data))) + len(e.Data)
	}
	n += len(e.unknownFields)
	return n
}

// The one-byte tags (field number << 3 | wire type) of the conf change messages' fields.
const (
	tagConfChangeID      = 1<<3 | 0 // id, varint
	tagConfChangeType    = 2<<3 | 0 // type, varint
	tagConfChangeNodeID  = 3<<3 | 0 // node_id, varint
	tagConfChangeContext = 4<<3 | 2 // context, bytes

	tagConfChangeSingleType   = 1<<3 | 0 // type, varint
	tagConfChangeSingleNodeID = 2<<3 | 0 // node_id, varint

	tagConfChangeV2Transition = 1<<3 | 0 // transition, varint
	tagConfChangeV2Changes    = 2<<3 | 2 // changes, bytes (a ConfChangeSingle)
	tagConfChangeV2Context    = 3<<3 | 2 // context, bytes
)

// appendVarint appends the varint encoding of v to b.
func appendVarint(b []byte, v uint64) []byte {
	for v >= 0x80 {
		b = append(b, byte(v)|0x80)
		v >>= 7
	}
	return append(b, byte(v))
}

// appendEnum appends the varint encoding of an enum value: its sign extension to 64 bits,
// as protobuf encodes an int32.
func appendEnum(b []byte, v int32) []byte {
	return appendVarint(b, uint64(int64(v)))
}

// appendBytes appends a length-delimited value: the length's varint, then the bytes.
func appendBytes(b []byte, v []byte) []byte {
	b = appendVarint(b, uint64(len(v)))
	return append(b, v...)
}

// MarshalConfChangeV1 returns the protobuf encoding of c's known fields, the bytes that
// protobuf-go's proto.Marshal(c) returns when c has no unknown fields: each set field, in
// field-number order (id, type, node_id, context), its tag then its value. Unknown fields are
// not encoded (raft never makes a conf change with unknown fields; a deliberate
// simplification). Nil if c is nil, a non-nil empty slice if no field is set, as
// proto.Marshal.
func MarshalConfChangeV1(c *ConfChange) []byte {
	if c == nil {
		return nil
	}
	b := []byte{}
	if c.Id != nil {
		b = append(b, tagConfChangeID)
		b = appendVarint(b, *c.Id)
	}
	if c.Type != nil {
		b = append(b, tagConfChangeType)
		b = appendEnum(b, int32(*c.Type))
	}
	if c.NodeId != nil {
		b = append(b, tagConfChangeNodeID)
		b = appendVarint(b, *c.NodeId)
	}
	if c.Context != nil {
		b = append(b, tagConfChangeContext)
		b = appendBytes(b, c.Context)
	}
	return b
}

// marshalConfChangeSingle returns the encoding of s's known fields (type, node_id); an empty
// slice if s is nil, as protobuf encodes a nil element of a repeated message field.
func marshalConfChangeSingle(s *ConfChangeSingle) []byte {
	b := []byte{}
	if s == nil {
		return b
	}
	if s.Type != nil {
		b = append(b, tagConfChangeSingleType)
		b = appendEnum(b, int32(*s.Type))
	}
	if s.NodeId != nil {
		b = append(b, tagConfChangeSingleNodeID)
		b = appendVarint(b, *s.NodeId)
	}
	return b
}

// MarshalConfChangeV2 returns the protobuf encoding of c's known fields, the bytes that
// protobuf-go's proto.Marshal(c) returns when c and its changes have no unknown fields: the
// transition if set, each change as a length-delimited field, the context if set. Unknown
// fields are not encoded (as MarshalConfChangeV1). Nil if c is nil, a non-nil empty slice if
// no field is set, as proto.Marshal.
func MarshalConfChangeV2(c *ConfChangeV2) []byte {
	if c == nil {
		return nil
	}
	b := []byte{}
	if c.Transition != nil {
		b = append(b, tagConfChangeV2Transition)
		b = appendEnum(b, int32(*c.Transition))
	}
	for _, s := range c.Changes {
		b = append(b, tagConfChangeV2Changes)
		b = appendBytes(b, marshalConfChangeSingle(s))
	}
	if c.Context != nil {
		b = append(b, tagConfChangeV2Context)
		b = appendBytes(b, c.Context)
	}
	return b
}

// errDecodeConfChange is the error of the conf change decoders on bytes they do not accept.
var errDecodeConfChange = errors.New("raftpb: invalid conf change encoding")

// consumeVarint decodes the varint at b[i:]: its value and the index after it. It accepts
// only what appendVarint produces: at most 10 bytes, the last one at most 1 if there are 10
// (the value fits 64 bits), and no trailing zero byte (the shortest encoding).
func consumeVarint(b []byte, i int) (uint64, int, bool) {
	var v uint64
	var shift uint64
	for n := 0; n < 10; n++ {
		if i >= len(b) {
			return 0, 0, false
		}
		c := b[i]
		i++
		if n == 9 && c > 1 {
			return 0, 0, false
		}
		v |= uint64(c&0x7f) << shift
		if c < 0x80 {
			if n > 0 && c == 0 {
				return 0, 0, false
			}
			return v, i, true
		}
		shift += 7
	}
	return 0, 0, false
}

// consumeEnum decodes the varint at b[i:] as an enum value: it accepts only the encoding of
// an int32 (appendEnum's), a value below 2^31 or the sign extension of a negative int32.
func consumeEnum(b []byte, i int) (int32, int, bool) {
	v, j, ok := consumeVarint(b, i)
	if !ok {
		return 0, 0, false
	}
	if v >= 1<<31 && v < 0xffffffff80000000 {
		return 0, 0, false
	}
	return int32(v), j, true
}

// consumeField returns the length-delimited value at b[i:] (a subslice of b, not a copy) and
// the index after it.
func consumeField(b []byte, i int) ([]byte, int, bool) {
	l, j, ok := consumeVarint(b, i)
	if !ok {
		return nil, 0, false
	}
	if l > uint64(len(b)-j) {
		return nil, 0, false
	}
	k := j + int(l)
	return b[j:k], k, true
}

// consumeBytes returns a fresh copy of the length-delimited value at b[i:] (non-nil, even if
// empty) and the index after it.
func consumeBytes(b []byte, i int) ([]byte, int, bool) {
	v, j, ok := consumeField(b, i)
	if !ok {
		return nil, 0, false
	}
	return append([]byte{}, v...), j, true
}

// UnmarshalConfChangeV1 resets c and decodes data into it. It accepts exactly the outputs of
// MarshalConfChangeV1 (each field at most once, in field-number order, shortest varints, an
// enum an int32's encoding) and decodes them back to the same fields (an unset field unset,
// the context a fresh slice, non-nil if set); it returns an error on any other bytes, leaving
// c with some of the fields. On the bytes it accepts, it agrees with proto.Unmarshal; it is
// stricter (protobuf-go also accepts other field orders, repeated fields, unknown fields).
// The empty input gives the empty message.
func UnmarshalConfChangeV1(data []byte, c *ConfChange) error {
	*c = ConfChange{}
	i := 0
	if i < len(data) && data[i] == tagConfChangeID {
		v, j, ok := consumeVarint(data, i+1)
		if !ok {
			return errDecodeConfChange
		}
		c.Id = new(v)
		i = j
	}
	if i < len(data) && data[i] == tagConfChangeType {
		v, j, ok := consumeEnum(data, i+1)
		if !ok {
			return errDecodeConfChange
		}
		c.Type = new(ConfChangeType(v))
		i = j
	}
	if i < len(data) && data[i] == tagConfChangeNodeID {
		v, j, ok := consumeVarint(data, i+1)
		if !ok {
			return errDecodeConfChange
		}
		c.NodeId = new(v)
		i = j
	}
	if i < len(data) && data[i] == tagConfChangeContext {
		v, j, ok := consumeBytes(data, i+1)
		if !ok {
			return errDecodeConfChange
		}
		c.Context = v
		i = j
	}
	if i != len(data) {
		return errDecodeConfChange
	}
	return nil
}

// unmarshalConfChangeSingle decodes data, exactly an output of marshalConfChangeSingle, into a
// new ConfChangeSingle; nil and false on any other bytes.
func unmarshalConfChangeSingle(data []byte) (*ConfChangeSingle, bool) {
	s := &ConfChangeSingle{}
	i := 0
	if i < len(data) && data[i] == tagConfChangeSingleType {
		v, j, ok := consumeEnum(data, i+1)
		if !ok {
			return nil, false
		}
		s.Type = new(ConfChangeType(v))
		i = j
	}
	if i < len(data) && data[i] == tagConfChangeSingleNodeID {
		v, j, ok := consumeVarint(data, i+1)
		if !ok {
			return nil, false
		}
		s.NodeId = new(v)
		i = j
	}
	if i != len(data) {
		return nil, false
	}
	return s, true
}

// UnmarshalConfChangeV2 resets c and decodes data into it. It accepts exactly the outputs of
// MarshalConfChangeV2 (the transition at most once, then any number of changes, then the
// context at most once; each change's fields at most once, in field-number order; shortest
// varints; an enum an int32's encoding) and decodes them back to the same fields (an unset
// field unset, the context a fresh slice, non-nil if set, Changes nil if there are none); it
// returns an error on any other bytes, leaving c with some of the fields. On the bytes it
// accepts, it agrees with proto.Unmarshal; it is stricter (as UnmarshalConfChangeV1). The
// empty input gives the empty message.
func UnmarshalConfChangeV2(data []byte, c *ConfChangeV2) error {
	*c = ConfChangeV2{}
	i := 0
	if i < len(data) && data[i] == tagConfChangeV2Transition {
		v, j, ok := consumeEnum(data, i+1)
		if !ok {
			return errDecodeConfChange
		}
		c.Transition = new(ConfChangeTransition(v))
		i = j
	}
	for i < len(data) && data[i] == tagConfChangeV2Changes {
		v, j, ok := consumeField(data, i+1)
		if !ok {
			return errDecodeConfChange
		}
		s, ok := unmarshalConfChangeSingle(v)
		if !ok {
			return errDecodeConfChange
		}
		c.Changes = append(c.Changes, s)
		i = j
	}
	if i < len(data) && data[i] == tagConfChangeV2Context {
		v, j, ok := consumeBytes(data, i+1)
		if !ok {
			return errDecodeConfChange
		}
		c.Context = v
		i = j
	}
	if i != len(data) {
		return errDecodeConfChange
	}
	return nil
}
