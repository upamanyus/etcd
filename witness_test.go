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
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	pb "go.etcd.io/raft/v3/raftpb"
)

// memWitnessStorage is a witness's durable state in memory, updated by
// compare-and-swap on a version number.
type memWitnessStorage struct {
	version uint64
	state   *pb.WitnessHardState
	writes  int
}

type memWitnessState struct {
	s       *memWitnessStorage
	version uint64
	state   *pb.WitnessHardState
}

func (s *memWitnessStorage) GetWitnessState() (WitnessState, error) {
	st := &pb.WitnessHardState{State: &pb.HardState{}}
	if s.state != nil {
		st = proto.Clone(s.state).(*pb.WitnessHardState)
	}
	return &memWitnessState{s: s, version: s.version, state: st}, nil
}

func (s *memWitnessStorage) Close() {}

func (w *memWitnessState) GetState() *pb.WitnessHardState { return w.state }

func (w *memWitnessState) Update(st *pb.WitnessHardState) error {
	if w.s.version != w.version {
		return ErrWitnessStateModified
	}
	w.s.state = proto.Clone(st).(*pb.WitnessHardState)
	w.s.version++
	w.s.writes++
	return nil
}

func witnessConfState() *pb.ConfState {
	return &pb.ConfState{Voters: []uint64{1, 2, 3}, Witness: new(uint64(3))}
}

// A node restarting in the joint configuration (1 2 3 4)&&(1 2 3), with witness
// 3 in both halves, restores its own ConfState.
func TestWitnessRestartInJointConfig(t *testing.T) {
	cs := &pb.ConfState{
		Voters: []uint64{1, 2, 3, 4}, VotersOutgoing: []uint64{1, 2, 3},
		Witness: new(uint64(3)), WitnessOutgoing: new(uint64(3)),
	}
	s := NewMemoryStorage()
	require.NoError(t, s.ApplySnapshot(&pb.Snapshot{Metadata: &pb.SnapshotMetadata{
		Index: new(uint64(10)), Term: new(uint64(1)), ConfState: cs}}))
	r := newTestRaft(1, 10, 1, s)
	require.Equal(t, [2]uint64{3, 3}, r.trk.Witnesses)
}

// A candidate whose log ends at a snapshot asks the witness for its vote, with
// the snapshot's term and subterm 0.
func TestWitnessCandidateAfterSnapshot(t *testing.T) {
	s := NewMemoryStorage()
	require.NoError(t, s.ApplySnapshot(&pb.Snapshot{Metadata: &pb.SnapshotMetadata{
		Index: new(uint64(10)), Term: new(uint64(1)), ConfState: witnessConfState()}}))
	r := newTestRaft(1, 10, 1, s)
	r.Step(&pb.Message{From: new(uint64(1)), To: new(uint64(1)), Type: pb.MsgHup.Enum()})
	require.Equal(t, StateCandidate, r.state)
	r.advanceMessagesAfterAppend()
	require.Len(t, r.witnessMsgs, 1)
	m := r.witnessMsgs[0]
	require.Equal(t, pb.MsgVote, m.Type)
	require.Equal(t, uint64(3), m.To)
	require.Equal(t, uint64(1), m.LastLogTerm)
	require.Equal(t, uint64(0), m.LastLogSubterm)
	require.Equal(t, uint64(10), m.LastLogIndex)
}

// A leader whose quorum-1 index in the replication set is compacted does not
// panic in maybeCommit.
func TestWitnessLeaderMaybeCommitCompacted(t *testing.T) {
	s := NewMemoryStorage()
	require.NoError(t, s.ApplySnapshot(&pb.Snapshot{Metadata: &pb.SnapshotMetadata{
		Index: new(uint64(10)), Term: new(uint64(1)),
		ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3, 4}, Witness: new(uint64(4))}}}))
	r := newTestRaft(1, 10, 1, s)
	r.becomeCandidate()
	r.becomeLeader()
	r.trk.Progress[3].RecentActive = false
	r.trk.Progress[2].RecentActive = true
	r.trk.Progress[4].RecentActive = true
	require.True(t, r.trk.ChangeReplicationSet())
	r.trk.Progress[2].Match = 10
	r.maybeCommit()
}

// The witness's answer to a request of an older term carries its term, so that
// the stale leader learns of it.
func TestWitnessOlderTermResponse(t *testing.T) {
	st := &memWitnessStorage{state: &pb.WitnessHardState{State: &pb.HardState{Term: new(uint64(5))}}}
	w := NewWitness(WitnessConfig{ID: 3, Storage: st, CheckQuorum: true})
	resp, err := w.Process(WitnessMessage{Type: pb.MsgHeartbeat, From: 1, To: 3, Term: 2})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, uint64(3), resp.GetFrom())
	require.Equal(t, uint64(5), resp.GetTerm())
}

// The witness's acknowledgement of a request that raised its term carries the
// new term, so that the leader does not drop it as stale.
func TestWitnessAppendNewerTermResponse(t *testing.T) {
	st := &memWitnessStorage{state: &pb.WitnessHardState{State: &pb.HardState{Term: new(uint64(1))}}}
	w := NewWitness(WitnessConfig{ID: 3, Storage: st})
	resp, err := w.Process(WitnessMessage{Type: pb.MsgApp, From: 1, To: 3, Term: 2,
		LastLogTerm: 2, LastLogSubterm: 1, LastLogIndex: 7, ReplicationSet: [2][]uint64{{1}, nil}})
	require.NoError(t, err)
	require.Equal(t, pb.MsgAppResp, resp.GetType())
	require.Equal(t, uint64(2), resp.GetTerm())
	require.Equal(t, uint64(7), resp.GetIndex())
	require.False(t, resp.GetReject())
}

// witnessCluster is two regular nodes (1 and 2) and the witness 3, each regular
// node with its own Witness on the shared witness storage.
type witnessCluster struct {
	t       *testing.T
	nodes   map[uint64]*RawNode
	stores  map[uint64]*MemoryStorage
	wits    map[uint64]Witness
	ws      *memWitnessStorage
	down    map[uint64]bool
	applied map[uint64][][]byte
	cut     bool // the network between 1 and 2 is cut (the witness storage is reachable)
	reads   map[uint64][]ReadState
}

func newWitnessCluster(t *testing.T) *witnessCluster {
	c := &witnessCluster{
		t: t, nodes: map[uint64]*RawNode{}, stores: map[uint64]*MemoryStorage{},
		wits: map[uint64]Witness{}, ws: &memWitnessStorage{}, down: map[uint64]bool{},
		applied: map[uint64][][]byte{}, reads: map[uint64][]ReadState{},
	}
	for _, id := range []uint64{1, 2} {
		s := NewMemoryStorage()
		require.NoError(t, s.ApplySnapshot(&pb.Snapshot{Metadata: &pb.SnapshotMetadata{
			Index: new(uint64(1)), Term: new(uint64(1)), ConfState: witnessConfState()}}))
		cfg := newTestConfig(id, 10, 1, s)
		cfg.CheckQuorum = true
		rn, err := NewRawNode(cfg)
		require.NoError(t, err)
		c.nodes[id], c.stores[id] = rn, s
		c.wits[id] = NewWitness(WitnessConfig{ID: 3, Storage: c.ws, CheckQuorum: true})
	}
	return c
}

// settle handles every Ready of the nodes that are up until none has one,
// dropping messages to and from nodes that are down.
func (c *witnessCluster) settle() {
	for range 1000 {
		progress := false
		for _, id := range []uint64{1, 2} {
			rn := c.nodes[id]
			if c.down[id] || !rn.HasReady() {
				continue
			}
			progress = true
			rd := rn.Ready()
			s := c.stores[id]
			if !IsEmptyHardState(rd.HardState) {
				require.NoError(c.t, s.SetHardState(rd.HardState))
			}
			require.NoError(c.t, s.Append(rd.Entries))
			for _, e := range rd.CommittedEntries {
				if e.GetType() == pb.EntryNormal && len(e.GetData()) > 0 {
					c.applied[id] = append(c.applied[id], e.GetData())
				}
			}
			var resps []*pb.Message
			c.reads[id] = append(c.reads[id], rd.ReadStates...)
			for _, m := range rd.Messages {
				if !c.down[m.GetTo()] && !c.cut {
					resps = append(resps, m)
				}
			}
			for _, m := range rd.WitnessMessages {
				resp, err := c.wits[id].Process(m)
				require.NoError(c.t, err)
				if resp != nil && resp.GetFrom() != 0 {
					resps = append(resps, resp)
				}
			}
			rn.Advance(rd)
			for _, m := range resps {
				_ = c.nodes[m.GetTo()].Step(m)
			}
		}
		if !progress {
			return
		}
	}
	c.t.Fatal("cluster did not settle")
}

func (c *witnessCluster) tick(id uint64, n int) {
	for range n {
		c.nodes[id].Tick()
		c.settle()
	}
}

func (c *witnessCluster) leader() uint64 {
	for _, id := range []uint64{1, 2} {
		if !c.down[id] && c.nodes[id].raft.state == StateLeader {
			return id
		}
	}
	return None
}

func (c *witnessCluster) hasApplied(id uint64, data []byte) bool {
	for _, d := range c.applied[id] {
		if bytes.Equal(d, data) {
			return true
		}
	}
	return false
}

// Two regular nodes and a witness: an entry committed with the witness while
// node 2 is down survives node 1 going down; node 2 alone cannot be elected.
func TestWitnessClusterCommitAndSafety(t *testing.T) {
	c := newWitnessCluster(t)
	require.NoError(t, c.nodes[1].Campaign())
	c.settle()
	require.Equal(t, uint64(1), c.leader())

	// With the full replication set, replication does not involve the witness.
	writes := c.ws.writes
	require.NoError(t, c.nodes[1].Propose([]byte("a")))
	c.settle()
	require.Equal(t, writes, c.ws.writes)
	require.True(t, c.hasApplied(1, []byte("a")))
	require.True(t, c.hasApplied(2, []byte("a")))

	// Node 2 goes down. At the next check-quorum the leader replaces it by the
	// witness in its replication set, and commits with the witness.
	c.down[2] = true
	c.tick(1, 25)
	require.Equal(t, uint64(1), c.leader())
	require.NoError(t, c.nodes[1].Propose([]byte("b")))
	c.settle()
	require.True(t, c.hasApplied(1, []byte("b")), "b committed with the witness")
	require.Greater(t, c.ws.writes, writes)
	writes = c.ws.writes

	// Shortcut replication: more entries in the same subterm do not write the
	// witness's storage.
	require.NoError(t, c.nodes[1].Propose([]byte("c")))
	c.settle()
	require.True(t, c.hasApplied(1, []byte("c")))
	require.Equal(t, writes, c.ws.writes)

	// Node 1 goes down, node 2 comes back: 2 lacks b, so the witness does not
	// vote for it.
	c.down[1] = true
	c.down[2] = false
	c.tick(2, 100)
	require.Equal(t, None, c.leader())
	require.False(t, c.hasApplied(2, []byte("b")))

	// Node 1 comes back: it is elected, and node 2 catches up.
	c.down[1] = false
	// (Node 1 still thinks itself the leader of its old term: wait for a leader
	// of the current term.)
	for range 100 {
		if l := c.leader(); l != None && c.nodes[l].raft.Term >= c.nodes[2].raft.Term {
			break
		}
		c.tick(1, 1)
		c.tick(2, 1)
	}
	require.Equal(t, uint64(1), c.leader())
	require.NoError(t, c.nodes[1].Propose([]byte("d")))
	c.settle()
	t.Logf("applied 1 %q 2 %q; commit 1 %d 2 %d", c.applied[1], c.applied[2],
		c.nodes[1].raft.raftLog.committed, c.nodes[2].raft.raftLog.committed)
	for _, d := range []string{"a", "b", "c", "d"} {
		require.True(t, c.hasApplied(1, []byte(d)), d)
		require.True(t, c.hasApplied(2, []byte(d)), d)
	}
}

// A leader cut off from the other node, but not from the witness's storage, must not confirm
// a read-index request with the witness's heartbeat response after the other node was elected
// in a newer term (with the witness's vote) and committed a write. raft#168's witness proxy
// answered the heartbeat from its cached state, so the read was answered at an index that
// missed the write; the witness now reads its storage for every heartbeat.
func TestWitnessStaleReadIndex(t *testing.T) {
	c := newWitnessCluster(t)
	require.NoError(t, c.nodes[1].Campaign())
	c.settle()
	require.Equal(t, uint64(1), c.leader())
	require.NoError(t, c.nodes[1].Propose([]byte("a")))
	c.settle()
	require.True(t, c.hasApplied(2, []byte("a")))

	// The network between 1 and 2 is cut. Node 2 times out and is elected in term 2 with
	// the witness's vote, then commits x with the witness (after replacing 1 in its
	// replication set at check-quorum).
	c.cut = true
	for range 100 {
		if c.nodes[2].raft.state == StateLeader {
			break
		}
		c.tick(2, 1)
	}
	require.Equal(t, StateLeader, c.nodes[2].raft.state)
	require.Equal(t, uint64(2), c.nodes[2].raft.Term)
	c.tick(2, 25)
	require.Equal(t, StateLeader, c.nodes[2].raft.state)
	require.NoError(t, c.nodes[2].Propose([]byte("x")))
	c.settle()
	require.True(t, c.hasApplied(2, []byte("x")), "x committed in term 2")

	// Node 1 still leads term 1 (it has not run a check-quorum since the cut). A read-index
	// request at node 1 must not be confirmed without x: the witness answers its heartbeat
	// with term 2, and node 1 steps down.
	require.Equal(t, StateLeader, c.nodes[1].raft.state)
	c.nodes[1].ReadIndex([]byte("r"))
	c.settle()
	require.True(t, len(c.reads[1]) == 0 || c.hasApplied(1, []byte("x")),
		"node 1 answered a read at index %v without x", c.reads[1])
	require.NotEqual(t, StateLeader, c.nodes[1].raft.state, "node 1 learned of term 2")
}
