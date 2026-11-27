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
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// The stale-ack scenarios: a member N (id 1) applied entries up to ci = 10 (term 4) after learning,
// in a later term, that they were committed, and crashed before saving that term. The boot step
// must bring N to at least that term; otherwise a stale candidate or leader C (id 3) whose log
// lacks 3..10 gets N's vote and N's acknowledgement of its commit index 10, beyond C's log, and C
// panics ("need non-empty snapshot") on its next send to N.

const (
	saN = uint64(1)
	saB = uint64(2)
	saC = uint64(3)
	saZ = uint64(4)
)

// confStorage is a MemoryStorage whose initial configuration is the five voters, with an empty
// snapshot (as a member's raft storage before its first snapshot file).
type confStorage struct {
	*raft.MemoryStorage
}

func (s confStorage) InitialState() (*raftpb.HardState, *raftpb.ConfState, error) {
	hs, _, err := s.MemoryStorage.InitialState()
	return hs, &raftpb.ConfState{Voters: []uint64{1, 2, 3, 4, 5}}, err
}

// staleLeader builds C's raft from its log terms and hard state.
func staleLeader(t *testing.T, terms []uint64, hs *raftpb.HardState) (*raft.RawNode, *raft.MemoryStorage) {
	ms := raft.NewMemoryStorage()
	var ents []*raftpb.Entry
	for i, term := range terms {
		ents = append(ents, &raftpb.Entry{Index: new(uint64(i + 1)), Term: new(term)})
	}
	require.NoError(t, ms.Append(ents))
	require.NoError(t, ms.SetHardState(hs))
	rn, err := raft.NewRawNode(&raft.Config{
		ID: saC, ElectionTick: 10, HeartbeatTick: 1, Storage: confStorage{ms},
		MaxSizePerMsg: 1 << 20, MaxInflightMsgs: 256,
	})
	require.NoError(t, err)
	return rn, ms
}

// cReady persists and returns C's next Ready's messages.
func cReady(t *testing.T, rn *raft.RawNode, ms *raft.MemoryStorage) []*raftpb.Message {
	if !rn.HasReady() {
		return nil
	}
	rd := rn.Ready()
	if !raft.IsEmptyHardState(rd.HardState) {
		require.NoError(t, ms.SetHardState(rd.HardState))
	}
	require.NoError(t, ms.Append(rd.Entries))
	rn.Advance(rd)
	return rd.Messages
}

// nReady persists and returns N's messages, waiting at most a moment for a Ready.
func nReady(t *testing.T, n raft.Node, ms *raft.MemoryStorage) []*raftpb.Message {
	var msgs []*raftpb.Message
	for {
		select {
		case rd := <-n.Ready():
			if !raft.IsEmptyHardState(rd.HardState) {
				require.NoError(t, ms.SetHardState(rd.HardState))
			}
			require.NoError(t, ms.Append(rd.Entries))
			msgs = append(msgs, rd.Messages...)
			n.Advance()
		case <-time.After(100 * time.Millisecond):
			return msgs
		}
	}
}

type staleAckOutcome struct {
	nTerm      uint64 // the term bootstrap hands N's raft
	voteGiven  bool   // N granted C's vote
	staleAck   uint64 // the largest index N acknowledged to C beyond C's last index (0: none)
	panicked   any    // C's panic
	cIsLeader  bool
	cLastIndex uint64
}

// runStaleAck boots N from img, then runs C (built from cTerms, cHS; its campaign reaches N, the
// other voters in grants answer it) and exchanges C's and N's messages for a few rounds.
func runStaleAck(t *testing.T, img catchUpImage, cTerms []uint64, cHS *raftpb.HardState, grants []uint64) (out staleAckOutcome) {
	cfg := catchUpCfg(t, t.TempDir(), false)
	writeCatchUpImage(t, cfg, img)
	r, b := bootCatchUp(t, cfg, true)
	defer b.Close()
	defer b.storage.wal.w.Close()
	out.nTerm = r.hs.GetTerm()
	n := raft.RestartNode(b.raft.config)
	defer n.Stop()
	nms := b.raft.storage
	nReady(t, n, nms) // N's restart Ready (its committed entries)

	c, cms := staleLeader(t, cTerms, cHS)
	defer func() {
		if p := recover(); p != nil {
			out.panicked = p
		}
		st := c.Status()
		out.cIsLeader = st.RaftState == raft.StateLeader
		out.cLastIndex, _ = cms.LastIndex()
	}()
	require.NoError(t, c.Campaign())
	toN := func(msgs []*raftpb.Message) {
		for _, m := range msgs {
			if m.GetTo() == saN {
				require.NoError(t, n.Step(context.TODO(), m))
			}
		}
	}
	fromN := func() {
		for _, m := range nReady(t, n, nms) {
			if m.GetTo() != saC {
				continue
			}
			switch m.GetType() {
			case raftpb.MessageType_MsgVoteResp:
				out.voteGiven = out.voteGiven || !m.GetReject()
			case raftpb.MessageType_MsgAppResp:
				last, _ := cms.LastIndex()
				if !m.GetReject() && m.GetIndex() > last && m.GetIndex() > out.staleAck {
					out.staleAck = m.GetIndex()
				}
			}
			_ = c.Step(m)
		}
	}
	msgs := cReady(t, c, cms)
	toN(msgs)
	term := c.Status().GetTerm()
	for _, g := range grants {
		require.NoError(t, c.Step(&raftpb.Message{Type: raftpb.MessageType_MsgVoteResp.Enum(),
			From: new(g), To: new(saC), Term: new(term)}))
	}
	fromN()
	for i := 0; i < 4; i++ {
		if c.Status().RaftState == raft.StateLeader {
			_ = c.Propose([]byte(fmt.Sprintf("prop%d", i)))
			c.Tick() // heartbeat
		}
		toN(cReady(t, c, cms))
		fromN()
	}
	return out
}

func checkStaleAck(t *testing.T, out staleAckOutcome, minTerm uint64) {
	t.Logf("N's term %d, vote given %v, stale ack %d, C leader %v (last index %d), C panicked: %v",
		out.nTerm, out.voteGiven, out.staleAck, out.cIsLeader, out.cLastIndex, out.panicked)
	require.Nil(t, out.panicked, "the stale leader panicked")
	require.Zero(t, out.staleAck, "N acknowledged an index beyond the stale leader's log")
	require.False(t, out.voteGiven, "N voted for a candidate whose log lacks its commit index")
	require.GreaterOrEqual(t, out.nTerm, minTerm, "N restarted below the term in which it learned its commit index")
}

// TestCatchUpRaftTermStaleCandidate is scenario S6: entries 3..10 at term 4 on A and N; A commits
// 10 at term 7 (votes A, B, Z); N learns it at term 7, applies through 10, and crashes before
// saving term 7: its WAL holds 1..10 with {Term 5, Commit 2}, its backend ci = 10 (term 4) with
// raft term 7. C (log 1@1 2@1 3@5, term 5) campaigns at term 6 with Z's vote; its delayed MsgVote
// reaches N. N must be at term 7, refuse it, and never acknowledge 10 to C.
func TestCatchUpRaftTermStaleCandidate(t *testing.T) {
	img := catchUpImage{
		walTerms: []uint64{1, 1, 4, 4, 4, 4, 4, 4, 4, 4},
		walHS:    &raftpb.HardState{Term: new(uint64(5)), Vote: new(uint64(0)), Commit: new(uint64(2))},
		ci:       10, ciTerm: 4, raftTerm: 7,
		members: []uint64{1, 2, 3, 4, 5},
	}
	out := runStaleAck(t, img, []uint64{1, 1, 5},
		&raftpb.HardState{Term: new(uint64(5)), Vote: new(saC), Commit: new(uint64(2))}, []uint64{saZ})
	checkStaleAck(t, out, 7)
}

// TestCatchUpRaftTermStaleLeader is scenario S5: C leads term 5 with the votes of B and Z (no
// CheckQuorum), its log 1@1 2@1 then its empty entry 3@5; A commits 10 at term 6; N learns it at
// term 6, applies through 10, and crashes before saving term 6: WAL {Term 5, Commit 2}, backend
// ci = 10 (term 4) with raft term 6. C's MsgApp reaches N. N must be at term 6 and never
// acknowledge 10 to C.
func TestCatchUpRaftTermStaleLeader(t *testing.T) {
	img := catchUpImage{
		walTerms: []uint64{1, 1, 4, 4, 4, 4, 4, 4, 4, 4},
		walHS:    &raftpb.HardState{Term: new(uint64(5)), Vote: new(uint64(0)), Commit: new(uint64(2))},
		ci:       10, ciTerm: 4, raftTerm: 6,
		members: []uint64{1, 2, 3, 4, 5},
	}
	out := runStaleAck(t, img, []uint64{1, 1},
		&raftpb.HardState{Term: new(uint64(4)), Commit: new(uint64(2))}, []uint64{saB, saZ})
	checkStaleAck(t, out, 6)
}

// TestCatchUpRaftTermRaisedWithCrashes: the S6 image through every crash point of the boot step's
// write: each start hands raft term 7 with no vote (the raft term is above the WAL's and the
// entry's), commit 10, and the WAL's entries.
func TestCatchUpRaftTermRaisedWithCrashes(t *testing.T) {
	img := catchUpImage{
		walTerms: []uint64{1, 1, 4, 4, 4, 4, 4, 4, 4, 4},
		walHS:    &raftpb.HardState{Term: new(uint64(5)), Vote: new(uint64(1)), Commit: new(uint64(2))},
		ci:       10, ciTerm: 4, raftTerm: 7,
		members: []uint64{1, 2, 3, 4, 5},
	}
	runCatchUpCase(t, img, catchUpExpect{
		snapIndex: 0, snapTerm: 0, snapVoters: []uint64{1, 2, 3, 4, 5},
		term: 7, vote: 0, commit: 10,
		entIndex: []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
	})
	// Branch B (the WAL ends below ci): the snapshot keeps ci's term, the hard state the raft term.
	img.walTerms = []uint64{1, 1, 4}
	img.walHS = &raftpb.HardState{Term: new(uint64(5)), Vote: new(uint64(1)), Commit: new(uint64(2))}
	runCatchUpCase(t, img, catchUpExpect{
		snapIndex: 10, snapTerm: 4, snapVoters: []uint64{1, 2, 3, 4, 5},
		term: 7, vote: 0, commit: 10,
	})
}

// TestCatchUpNoRaftTermRefused: a backend ahead of the WAL without a raft term saved with its
// consistent index (written by a binary that does not save it, or by one that saved it with an
// earlier index and then moved the consistent index without it) is refused: the term in which ci
// was committed is unknown, and raising the commit index without it is the stale-ack panic above.
func TestCatchUpNoRaftTermRefused(t *testing.T) {
	for _, c := range []struct {
		name                    string
		raftTerm, raftTermIndex uint64
	}{
		{"absent", 0, 0},
		{"saved with an earlier index", 9, 8},
	} {
		t.Run(c.name, func(t *testing.T) {
			cfg := catchUpCfg(t, t.TempDir(), false)
			writeCatchUpImage(t, cfg, catchUpImage{
				walTerms: []uint64{1, 1, 4, 4, 4, 4, 4, 4, 4, 4},
				walHS:    &raftpb.HardState{Term: new(uint64(5)), Commit: new(uint64(2))},
				ci:       10, ciTerm: 4, raftTerm: c.raftTerm, raftTermIndex: c.raftTermIndex,
				members: []uint64{1, 2, 3, 4, 5},
			})
			_, err := bootstrap(cfg)
			require.ErrorContains(t, err, "no raft term")
		})
	}
	// A backend within the WAL's commit needs no raft term (a graceful restart, or a backend
	// written before the upgrade).
	cfg := catchUpCfg(t, t.TempDir(), false)
	writeCatchUpImage(t, cfg, catchUpImage{
		walTerms: []uint64{1, 1, 4, 4, 4, 4, 4, 4, 4, 4},
		walHS:    &raftpb.HardState{Term: new(uint64(5)), Commit: new(uint64(10))},
		ci:       10, ciTerm: 4,
		members: []uint64{1, 2, 3, 4, 5},
	})
	r, _ := bootCatchUp(t, cfg, false)
	require.Equal(t, uint64(5), r.hs.GetTerm())
	require.Equal(t, uint64(10), r.hs.GetCommit())
}
