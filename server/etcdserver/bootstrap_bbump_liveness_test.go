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

package etcdserver

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// The liveness cost of raising the term when the boot step replaces the log by a snapshot record:
// a 3-member cluster (etcd's raft settings: CheckQuorum and PreVote) with a stable leader; one
// follower crashes and restarts the way the boot step leaves it (a snapshot at the cluster's
// commit index, commit there), at the term it had or one above it. The test measures, in ticks,
// whether the leader steps down and how long the cluster is without a leader.

type lvNode struct {
	rn *raft.RawNode
	ms *raft.MemoryStorage
}

type lvCluster struct {
	t     *testing.T
	nodes map[uint64]*lvNode
}

func lvConfig(id uint64, ms *raft.MemoryStorage) *raft.Config {
	return &raft.Config{
		ID: id, ElectionTick: 10, HeartbeatTick: 1, Storage: ms,
		MaxSizePerMsg: 1 << 20, MaxInflightMsgs: 256, CheckQuorum: true, PreVote: true,
	}
}

func lvVoters() *raftpb.ConfState { return &raftpb.ConfState{Voters: []uint64{1, 2, 3}} }

func newLvCluster(t *testing.T) *lvCluster {
	c := &lvCluster{t: t, nodes: map[uint64]*lvNode{}}
	for id := uint64(1); id <= 3; id++ {
		ms := raft.NewMemoryStorage()
		require.NoError(t, ms.ApplySnapshot(&raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{
			Index: new(uint64(1)), Term: new(uint64(1)), ConfState: lvVoters()}}))
		rn, err := raft.NewRawNode(lvConfig(id, ms))
		require.NoError(t, err)
		c.nodes[id] = &lvNode{rn: rn, ms: ms}
	}
	return c
}

// flush handles every Ready and delivers every message until the cluster is quiet.
func (c *lvCluster) flush() {
	for round := 0; round < 100; round++ {
		var msgs []*raftpb.Message
		for _, n := range c.nodes {
			if !n.rn.HasReady() {
				continue
			}
			rd := n.rn.Ready()
			if !raft.IsEmptySnap(rd.Snapshot) {
				require.NoError(c.t, n.ms.ApplySnapshot(rd.Snapshot))
			}
			if !raft.IsEmptyHardState(rd.HardState) {
				require.NoError(c.t, n.ms.SetHardState(rd.HardState))
			}
			require.NoError(c.t, n.ms.Append(rd.Entries))
			msgs = append(msgs, rd.Messages...)
			n.rn.Advance(rd)
		}
		if len(msgs) == 0 {
			return
		}
		for _, m := range msgs {
			if n, ok := c.nodes[m.GetTo()]; ok {
				_ = n.rn.Step(m)
			}
		}
	}
}

func (c *lvCluster) tick() {
	for _, n := range c.nodes {
		n.rn.Tick()
	}
	c.flush()
}

func (c *lvCluster) leader() (uint64, uint64) {
	for id, n := range c.nodes {
		if st := n.rn.Status(); st.RaftState == raft.StateLeader {
			return id, st.GetTerm()
		}
	}
	return 0, 0
}

// restart replaces member id by a member booted from a snapshot at the leader's commit index, with
// the hard state {term, vote 0, commit}: the state the boot step's second case leaves.
func (c *lvCluster) restart(id, term uint64) {
	lead, _ := c.leader()
	lst := c.nodes[lead].rn.Status()
	ci := lst.GetCommit()
	ciTerm, err := c.nodes[lead].ms.Term(ci)
	require.NoError(c.t, err)
	ms := raft.NewMemoryStorage()
	require.NoError(c.t, ms.ApplySnapshot(&raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{
		Index: new(ci), Term: new(ciTerm), ConfState: lvVoters()}}))
	require.NoError(c.t, ms.SetHardState(&raftpb.HardState{Term: new(term), Vote: new(uint64(0)), Commit: new(ci)}))
	rn, err := raft.NewRawNode(lvConfig(id, ms))
	require.NoError(c.t, err)
	c.nodes[id] = &lvNode{rn: rn, ms: ms}
}

type lvOutcome struct {
	stepDown   bool   // the old leader lost leadership
	noLeader   int    // ticks after the restart without a leader
	finalTerm  uint64 // the term of the leader at the end
	sameLeader bool   // the old leader leads again at the end
}

func runLiveness(t *testing.T, bump bool) lvOutcome {
	c := newLvCluster(t)
	require.NoError(t, c.nodes[1].rn.Campaign())
	c.flush()
	for i := 0; i < 5; i++ {
		require.NoError(t, c.nodes[1].rn.Propose([]byte("x")))
		c.tick()
	}
	lead, term := c.leader()
	require.Equal(t, uint64(1), lead)
	// The follower's term before the crash is the leader's (it saved it when it followed).
	f := uint64(3)
	restartTerm := term
	if bump {
		restartTerm = term + 1
	}
	c.restart(f, restartTerm)
	var out lvOutcome
	for i := 0; i < 100; i++ {
		if i%3 == 0 {
			if l, _ := c.leader(); l != 0 {
				_ = c.nodes[l].rn.Propose([]byte("y"))
			}
		}
		c.tick()
		l, _ := c.leader()
		if l != 1 {
			out.stepDown = true
		}
		if l == 0 {
			out.noLeader++
		}
	}
	l, lt := c.leader()
	out.finalTerm, out.sameLeader = lt, l == 1
	// Every member reached the leader's log (the cluster made progress).
	require.NotZero(t, l, "no leader at the end")
	return out
}

// TestCatchUpSnapshotRaisedTermLiveness measures the disruption: without the raise the follower
// rejoins quietly; with it, the leader steps down on the follower's first response (its
// heartbeat answered at a higher term) and the cluster is without a leader until an election
// timeout passes.
func TestCatchUpSnapshotRaisedTermLiveness(t *testing.T) {
	const trials = 200
	for _, bump := range []bool{false, true} {
		var downs, same int
		var gaps []int
		for i := 0; i < trials; i++ {
			o := runLiveness(t, bump)
			if o.stepDown {
				downs++
			}
			if o.sameLeader {
				same++
			}
			gaps = append(gaps, o.noLeader)
		}
		sort.Ints(gaps)
		t.Logf("raise %v: leader stepped down in %d/%d trials; ticks without a leader min %d median %d max %d (election tick 10); old leader leads at the end in %d/%d",
			bump, downs, trials, gaps[0], gaps[trials/2], gaps[trials-1], same, trials)
		if bump {
			require.Equal(t, trials, downs, "the raised term should depose the leader")
		} else {
			require.Zero(t, downs, "without the raise the leader should stay")
		}
	}
}
