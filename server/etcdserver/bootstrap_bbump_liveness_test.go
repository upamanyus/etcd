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

// restart replaces member id by a member booted from a snapshot at the cluster's commit index,
// with the hard state {term, vote, commit}: the state the boot step's second case leaves.
func (c *lvCluster) restart(id, term, vote uint64) {
	var src *lvNode
	var ci uint64
	for _, n := range c.nodes {
		if cm := n.rn.Status().GetCommit(); src == nil || cm > ci {
			src, ci = n, cm
		}
	}
	ciTerm, err := src.ms.Term(ci)
	require.NoError(c.t, err)
	ms := raft.NewMemoryStorage()
	require.NoError(c.t, ms.ApplySnapshot(&raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{
		Index: new(ci), Term: new(ciTerm), ConfState: lvVoters()}}))
	require.NoError(c.t, ms.SetHardState(&raftpb.HardState{Term: new(term), Vote: new(vote), Commit: new(ci)}))
	rn, err := raft.NewRawNode(lvConfig(id, ms))
	require.NoError(c.t, err)
	c.nodes[id] = &lvNode{rn: rn, ms: ms}
}

// The boot step's rules for the second case's term and vote, when the WAL's term is at or above
// ci's term and the raft term (so that only the rule decides).
type lvRule int

const (
	lvKeep   lvRule = iota // approved fix (c): keep Term_w and Vote_w
	lvAlways               // candidate (iii): Term_w + 1, no vote
	lvOwn                  // the narrower rule: Term_w + 1, no vote, only when Vote_w is the member's own id
)

func (r lvRule) String() string {
	return [...]string{"keep (c)", "always raise (iii)", "raise iff own vote"}[r]
}

func (r lvRule) apply(id, term, vote uint64) (uint64, uint64) {
	if r == lvAlways || (r == lvOwn && vote == id) {
		return term + 1, 0
	}
	return term, vote
}

// The member that restarts, by its role before the crash.
type lvScenario int

const (
	lvFollower     lvScenario = iota // a follower of the stable leader (it voted for the leader)
	lvLeader                         // the leader (it voted for itself)
	lvLostCampaign                   // a member that campaigned in the leader's term and lost
)

func (s lvScenario) String() string {
	return [...]string{"follower", "leader", "lost candidate of the current term"}[s]
}

type lvOutcome struct {
	stepDown bool // the stable leader stopped leading (it crashed, in the leader scenario)
	noLeader int  // ticks after the restart without a leader
}

// lvSetup elects member 1 leader; in lvLostCampaign member 3 campaigns in the same term and loses
// (both campaign at once; member 2's vote decides; retried until 1 wins).
func lvSetup(t *testing.T, sc lvScenario) *lvCluster {
	for attempt := 0; attempt < 1000; attempt++ {
		c := newLvCluster(t)
		require.NoError(t, c.nodes[1].rn.Campaign())
		if sc == lvLostCampaign {
			require.NoError(t, c.nodes[3].rn.Campaign())
		}
		c.flush()
		for i := 0; i < 5; i++ {
			if l, _ := c.leader(); l != 0 {
				_ = c.nodes[l].rn.Propose([]byte("x"))
			}
			c.tick()
		}
		lead, term := c.leader()
		if lead != 1 {
			continue
		}
		if sc == lvLostCampaign {
			st := c.nodes[3].rn.Status()
			if st.GetTerm() != term || st.GetVote() != 3 {
				continue
			}
		}
		return c
	}
	t.Fatal("no setup")
	return nil
}

func runLiveness(t *testing.T, sc lvScenario, rule lvRule) lvOutcome {
	c := lvSetup(t, sc)
	id := uint64(3)
	if sc == lvLeader {
		id = 1
	}
	st := c.nodes[id].rn.Status()
	term, vote := rule.apply(id, st.GetTerm(), st.GetVote())
	if sc == lvFollower {
		require.Equal(t, uint64(1), st.GetVote())
	} else {
		require.Equal(t, id, st.GetVote())
	}
	c.restart(id, term, vote)
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
	l, _ := c.leader()
	require.NotZero(t, l, "no leader at the end")
	return out
}

// TestCatchUpSnapshotRaisedTermLiveness measures the disruption of each rule, per restarted
// member: a follower restarted at a raised term answers the leader's heartbeat at that term and
// the leader steps down; the cluster is without a leader until an election timeout passes.
func TestCatchUpSnapshotRaisedTermLiveness(t *testing.T) {
	const trials = 200
	for _, sc := range []lvScenario{lvFollower, lvLeader, lvLostCampaign} {
		for _, rule := range []lvRule{lvKeep, lvAlways, lvOwn} {
			var downs int
			var gaps []int
			for i := 0; i < trials; i++ {
				o := runLiveness(t, sc, rule)
				if o.stepDown {
					downs++
				}
				gaps = append(gaps, o.noLeader)
			}
			sort.Ints(gaps)
			t.Logf("LIVENESS | %s | %s | leader lost %d/%d | ticks without a leader min %d median %d max %d",
				sc, rule, downs, trials, gaps[0], gaps[trials/2], gaps[trials-1])
			if sc == lvFollower {
				if rule == lvAlways {
					require.Equal(t, trials, downs)
				} else {
					require.Zero(t, downs)
				}
			}
		}
	}
}
