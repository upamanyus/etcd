// Copyright 2019 The etcd Authors
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

import (
	"fmt"
	"slices"

	"google.golang.org/protobuf/proto"
)

// Equivalent returns a nil error if the inputs describe the same configuration.
// On mismatch, returns a descriptive error showing the differences.
func (cs *ConfState) Equivalent(cs2 *ConfState) error {
	if cs == nil || cs2 == nil {
		return fmt.Errorf("cannot compare ConfState: nil input (left=%v, right=%v)", cs == nil, cs2 == nil)
	}

	cs1v := proto.Clone(cs).(*ConfState)
	cs2v := proto.Clone(cs2).(*ConfState)

	s := func(sl *[]uint64) {
		slices.Sort(*sl)
	}
	for _, c := range []*ConfState{cs1v, cs2v} {
		s(&c.Voters)
		s(&c.Learners)
		s(&c.VotersOutgoing)
		s(&c.LearnersNext)

		// Treat nil AutoLeave as false.
		autoLeave := c.GetAutoLeave()
		c.AutoLeave = &autoLeave
	}

	// Compare the configurations, not the messages: proto.Equal also compares
	// unknown fields, so a conf state that arrived with one (a snapshot from a
	// peer running a newer version) would never be equivalent to the
	// configuration it describes, and assertConfStatesEquivalent would panic.
	if !confStatesEqual(cs1v, cs2v) {
		return fmt.Errorf("ConfStates not equivalent after sorting:\n%+#v\n%+#v\nInputs were:\n%+#v\n%+#v", cs1v, cs2v, cs, cs2)
	}
	return nil
}

// confStatesEqual reports whether two ConfStates, their id slices sorted and
// their AutoLeave set, hold the same configuration.
func confStatesEqual(a, b *ConfState) bool {
	return idsEqual(a.Voters, b.Voters) && idsEqual(a.Learners, b.Learners) &&
		idsEqual(a.VotersOutgoing, b.VotersOutgoing) &&
		idsEqual(a.LearnersNext, b.LearnersNext) &&
		boolOf(a.AutoLeave) == boolOf(b.AutoLeave)
}

// boolOf reads an optional bool field, an unset one as false.
func boolOf(p *bool) bool {
	return p != nil && *p
}

// idsEqual reports whether two id slices hold the same ids in the same order
// (a nil and an empty slice alike).
func idsEqual(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
