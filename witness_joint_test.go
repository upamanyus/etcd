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

package raft

import (
	"testing"

	"github.com/stretchr/testify/require"

	pb "go.etcd.io/raft/v3/raftpb"
)

// changeRSUntil calls ChangeReplicationSet until the epoch's excluded voters are ex0 and ex1
// (the voter it excludes depends on map iteration order), and starts the subterm with its
// empty entry, as maybeStartNewSubterm does.
func changeRSUntil(t *testing.T, r *raft, ex0, ex1 uint64) {
	t.Helper()
	old := r.trk.Epoch
	for range 10000 {
		r.trk.Epoch = old
		if r.trk.ChangeReplicationSet() &&
			r.trk.Epoch.ReplicationSet[0].Excluded == ex0 && r.trk.Epoch.ReplicationSet[1].Excluded == ex1 {
			require.True(t, r.appendEntry(&pb.Entry{}))
			return
		}
	}
	t.Fatalf("ChangeReplicationSet never excluded %d and %d", ex0, ex1)
}

// The joint configuration (1 2 3 4 5) && (1 2 5), witness 5 in both halves. The leader 1's
// replication sets come to exclude the witness in the incoming half and voter 2 in the
// outgoing half. Then an entry acknowledged by 1 alone (quorum-1 of the outgoing half's set) was
// sent to the witness, which stored it with the incoming half's set (1 2 3 4), and the entry
// committed with 4's acknowledgement: incoming (1 4 5), outgoing (1 5). Node 2, without the
// entry, then got 3's vote and the witness's: elected without a committed entry. The witness
// must be sent an entry only when every half it is the witness of has it in its replication
// set.
func TestWitnessJointOutgoingOnlyReplication(t *testing.T) {
	cs := &pb.ConfState{Voters: []uint64{1, 2, 3, 4, 5}, VotersOutgoing: []uint64{1, 2, 5},
		Witness: new(uint64(5)), WitnessOutgoing: new(uint64(5))}
	snap := &pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: new(uint64(1)), Term: new(uint64(1)), ConfState: cs}}
	s1 := NewMemoryStorage()
	require.NoError(t, s1.ApplySnapshot(snap))
	r1 := newTestRaft(1, 10, 1, s1)
	r1.becomeCandidate()
	r1.becomeLeader()
	require.Equal(t, [2]uint64{5, 5}, [2]uint64{r1.trk.Epoch.ReplicationSet[0].Excluded,
		r1.trk.Epoch.ReplicationSet[1].Excluded})

	active := func(ids ...uint64) {
		for id, pr := range r1.trk.Progress {
			pr.RecentActive = false
			for _, a := range ids {
				if a == id {
					pr.RecentActive = true
				}
			}
		}
	}
	// 2 and 3 unresponsive: the incoming half excludes 3, the outgoing half 2
	active(1, 4, 5)
	changeRSUntil(t, r1, 3, 2)
	// 3 back, the witness unresponsive (2 still): the incoming half's 3 is replaced by the
	// witness, the outgoing half keeps 2 excluded
	r1.trk.Progress[3].BecomeReplicate()
	active(1, 3, 4)
	changeRSUntil(t, r1, 5, 2)

	// the entry, acknowledged by 4
	require.NoError(t, r1.Step(&pb.Message{From: new(uint64(1)), To: new(uint64(1)),
		Type: pb.MsgProp.Enum(), Entries: []*pb.Entry{{Data: []byte("e")}}}))
	idx := r1.raftLog.lastIndex()
	// the leader's own acknowledgements of its entries
	r1.advanceMessagesAfterAppend()
	ws := &memWitnessStorage{}
	w1 := NewWitness(WitnessConfig{ID: 5, Storage: ws, CheckQuorum: true})
	r1.witnessMsgs = nil
	require.NoError(t, r1.Step(&pb.Message{From: new(uint64(4)), To: new(uint64(1)),
		Type: pb.MsgAppResp.Enum(), Term: new(r1.Term), Index: new(idx)}))
	for _, m := range r1.witnessMsgs {
		resp, err := w1.Process(m)
		require.NoError(t, err)
		if resp != nil {
			require.NoError(t, r1.Step(resp))
		}
	}
	committed := r1.raftLog.committed >= idx

	// node 2, with the log up to the entry before it, campaigns
	ents, err := r1.raftLog.entries(2, noLimit)
	require.NoError(t, err)
	s2 := NewMemoryStorage()
	require.NoError(t, s2.ApplySnapshot(snap))
	var prior []*pb.Entry
	for _, e := range ents {
		if e.GetIndex() < idx {
			prior = append(prior, e)
		}
	}
	require.NoError(t, s2.Append(prior))
	r2 := newTestRaft(2, 10, 1, s2)
	r2.Term = r1.Term
	require.NoError(t, r2.Step(&pb.Message{From: new(uint64(2)), To: new(uint64(2)), Type: pb.MsgHup.Enum()}))
	r2.advanceMessagesAfterAppend()
	require.NoError(t, r2.Step(&pb.Message{From: new(uint64(3)), To: new(uint64(2)),
		Type: pb.MsgVoteResp.Enum(), Term: new(r2.Term)}))
	w2 := NewWitness(WitnessConfig{ID: 5, Storage: ws, CheckQuorum: true})
	for _, m := range r2.witnessMsgs {
		resp, err := w2.Process(m)
		require.NoError(t, err)
		if resp != nil {
			require.NoError(t, r2.Step(resp))
		}
	}
	t.Logf("idx %d committed %v (commit %d), r1 witnessMsgs %d, ws %v; r2 %v term %d witnessMsgs %d votes %v",
		idx, committed, r1.raftLog.committed, len(r1.witnessMsgs), ws.state, r2.state, r2.Term,
		len(r2.witnessMsgs), r2.trk.Votes)
	t1, _ := r1.raftLog.term(idx)
	t2, _ := r2.raftLog.term(idx)
	require.False(t, committed && r2.state == StateLeader && t2 != t1,
		"node 2 elected in term %d without the entry %d (term %d) node 1 committed: its entry there is of term %d",
		r2.Term, idx, t1, t2)
}
