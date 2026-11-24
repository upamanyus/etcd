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

package wal

import (
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"google.golang.org/protobuf/proto"

	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// tornSave writes ents as Save does, then stops where a crash can stop it: the entry
// records are written and synced, the state record that ends the Save is not.
func tornSave(t *testing.T, w *WAL, ents []*raftpb.Entry) {
	t.Helper()
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, e := range ents {
		require.NoError(t, w.saveEntry(e))
	}
	require.NoError(t, w.sync())
}

func ent(index, term uint64, data string) *raftpb.Entry {
	return &raftpb.Entry{Index: new(index), Term: new(term), Type: raftpb.EntryNormal.Enum(), Data: []byte(data)}
}

func hs(term, vote, commit uint64) *raftpb.HardState {
	return &raftpb.HardState{Term: new(term), Vote: new(vote), Commit: new(commit)}
}

func describe(ents []*raftpb.Entry) []string {
	var out []string
	for _, e := range ents {
		out = append(out, raft.DescribeEntry(e, nil))
	}
	return out
}

func readAllAt0(t *testing.T, dir string) (*WAL, *raftpb.HardState, []*raftpb.Entry) {
	t.Helper()
	w, err := Open(zaptest.NewLogger(t), dir, &walpb.Snapshot{})
	require.NoError(t, err)
	_, st, ents, err := w.ReadAll()
	require.NoError(t, err)
	return w, st, ents
}

// A Save interrupted after some or all of its entry records (and before its state record)
// reads back as no Save, also when its entries overwrite earlier ones; the next Save writes
// over its records, which no later read sees.
func TestTornSaveReadsAsNoSave(t *testing.T) {
	for _, torn := range [][]*raftpb.Entry{
		{ent(3, 1, "c")},
		{ent(3, 1, "c"), ent(4, 1, "d")},
		{ent(2, 2, "B")}, // overwrites index 2 (a follower's conflicting append)
		{ent(2, 2, "B"), ent(3, 2, "C")},
	} {
		dir := t.TempDir()
		w, err := Create(zaptest.NewLogger(t), dir, []byte("meta"))
		require.NoError(t, err)
		require.NoError(t, w.Save(hs(1, 1, 1), []*raftpb.Entry{ent(1, 1, "a"), ent(2, 1, "b")}))
		tornSave(t, w, torn)
		require.NoError(t, w.Close())

		w, st, ents := readAllAt0(t, dir)
		assert.True(t, proto.Equal(hs(1, 1, 1), st), "state %v", st)
		assert.Equal(t, describe([]*raftpb.Entry{ent(1, 1, "a"), ent(2, 1, "b")}), describe(ents))

		// The next Save (its hard state unchanged: empty) writes over the torn records.
		require.NoError(t, w.Save(&raftpb.HardState{}, []*raftpb.Entry{ent(3, 1, "x")}))
		require.NoError(t, w.Close())
		w, st, ents = readAllAt0(t, dir)
		assert.True(t, proto.Equal(hs(1, 1, 1), st), "state %v", st)
		assert.Equal(t, describe([]*raftpb.Entry{ent(1, 1, "a"), ent(2, 1, "b"), ent(3, 1, "x")}), describe(ents))
		require.NoError(t, w.Close())
	}
}

// A Save with entries and an empty hard state ends with a state record of the current hard
// state (an empty one before any): its entries count.
func TestSaveEntriesEmptyHardStateWritesState(t *testing.T) {
	dir := t.TempDir()
	w, err := Create(zaptest.NewLogger(t), dir, []byte("meta"))
	require.NoError(t, err)
	require.NoError(t, w.Save(&raftpb.HardState{}, []*raftpb.Entry{ent(1, 1, "a")}))
	require.NoError(t, w.Close())
	w, st, ents := readAllAt0(t, dir)
	assert.True(t, raft.IsEmptyHardState(st))
	assert.Len(t, ents, 1)
	unsaved, err := Unsaved(zaptest.NewLogger(t), dir)
	require.NoError(t, err)
	assert.False(t, unsaved)
	require.NoError(t, w.Close())
}

// Unsaved: a WAL as Create leaves it, or with the entry records of a torn first Save, holds
// nothing saved, and DiscardUnsaved removes it (to Create's temporary directory); one Save
// makes it saved.
func TestUnsaved(t *testing.T) {
	lg := zaptest.NewLogger(t)
	for _, tc := range []struct {
		name    string
		write   func(w *WAL)
		unsaved bool
	}{
		{"created", func(w *WAL) {}, true},
		{"torn first Save", func(w *WAL) { tornSave(t, w, []*raftpb.Entry{ent(1, 1, "a"), ent(2, 1, "b")}) }, true},
		{"first Save", func(w *WAL) { require.NoError(t, w.Save(hs(1, 0, 2), []*raftpb.Entry{ent(1, 1, "a"), ent(2, 1, "b")})) }, false},
		{"state only", func(w *WAL) { require.NoError(t, w.Save(hs(1, 0, 0), nil)) }, false},
		{"snapshot", func(w *WAL) {
			require.NoError(t, w.SaveSnapshot(&walpb.Snapshot{Index: new(uint64(5)), Term: new(uint64(1)), ConfState: &raftpb.ConfState{}}))
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "wal")
			w, err := Create(lg, dir, []byte("meta"))
			require.NoError(t, err)
			tc.write(w)
			require.NoError(t, w.Close())

			unsaved, err := Unsaved(lg, dir)
			require.NoError(t, err)
			assert.Equal(t, tc.unsaved, unsaved)
			discarded, err := DiscardUnsaved(lg, dir)
			require.NoError(t, err)
			assert.Equal(t, tc.unsaved, discarded)
			assert.Equal(t, !tc.unsaved, Exist(dir))
			if tc.unsaved {
				_, err := os.Stat(dir + ".tmp")
				require.NoError(t, err)
				// Create starts afresh over the leftover temporary directory.
				w, err := Create(lg, dir, []byte("meta"))
				require.NoError(t, err)
				require.NoError(t, w.Close())
				unsaved, err := Unsaved(lg, dir)
				require.NoError(t, err)
				assert.True(t, unsaved)
			}
		})
	}
}

// initStorage is a MemoryStorage whose initial configuration is the voters given (the
// configuration of an initial snapshot at index 0, as raft's own tests set it up).
type initStorage struct {
	*raft.MemoryStorage
	cs *raftpb.ConfState
}

func (s *initStorage) InitialState() (*raftpb.HardState, *raftpb.ConfState, error) {
	st, _, err := s.MemoryStorage.InitialState()
	return st, s.cs, err
}

// TestRestartWithEntriesButNotCommit is raft's (issues: a crash after a Ready's entries are
// written but before its HardState lets two nodes commit different entries at one index) with
// node 1 persisting to a WAL and restarting from it, as etcd does. Five RawNodes with etcd's
// settings (PreVote, CheckQuorum); voters 1, 2, 3, then 4 and 5 added while node 1 is cut off.
// Node 1 receives both conf changes and commit index 3 in one Ready, and crashes in its Save
// after the entry records are written and before the state record is: the Save reads back as
// none, so node 1 restarts without the entries, cannot win an election in its old
// configuration, and no index is committed with two different entries.
func TestRestartWithEntriesButNotCommit(t *testing.T) {
	lg := zaptest.NewLogger(t)
	type member struct {
		rn        *raft.RawNode
		s         *initStorage
		committed map[uint64]*raftpb.Entry
	}
	voters := &raftpb.ConfState{Voters: []uint64{1, 2, 3}}
	cfg := func(id uint64, s raft.Storage) *raft.Config {
		return &raft.Config{
			ID: id, ElectionTick: 10, HeartbeatTick: 1, Storage: s,
			MaxSizePerMsg: math.MaxUint64, MaxInflightMsgs: 256,
			PreVote: true, CheckQuorum: true,
		}
	}
	ms := map[uint64]*member{}
	for id := uint64(1); id <= 5; id++ {
		s := &initStorage{MemoryStorage: raft.NewMemoryStorage(), cs: voters}
		rn, err := raft.NewRawNode(cfg(id, s))
		require.NoError(t, err)
		ms[id] = &member{rn: rn, s: s, committed: map[uint64]*raftpb.Entry{}}
	}
	dir := t.TempDir()
	w, err := Create(lg, dir, []byte("meta"))
	require.NoError(t, err)
	defer func() { w.Close() }()

	side := map[uint64]int{} // nodes talk only to nodes on the same side
	crash := false           // node 1 crashes in the middle of its next Save with entries
	var crashed raft.Ready
	stabilize := func() {
		for busy := true; busy; {
			busy = false
			var msgs []*raftpb.Message
			for id := uint64(1); id <= 5; id++ {
				m := ms[id]
				if !m.rn.HasReady() {
					continue
				}
				busy = true
				rd := m.rn.Ready()
				if id == 1 {
					if crash && len(rd.Entries) > 0 {
						tornSave(t, w, rd.Entries)
						require.NoError(t, w.Close())
						crash, crashed = false, rd
						// Restart from the WAL, as etcd's bootstrap does.
						var st *raftpb.HardState
						var ents []*raftpb.Entry
						w, st, ents = readAllAt0(t, dir)
						s := &initStorage{MemoryStorage: raft.NewMemoryStorage(), cs: voters}
						require.NoError(t, s.SetHardState(st))
						require.NoError(t, s.Append(ents))
						rn, err := raft.NewRawNode(cfg(1, s))
						require.NoError(t, err)
						m.rn, m.s = rn, s
						continue
					}
					require.NoError(t, w.Save(rd.HardState, rd.Entries))
				}
				if !raft.IsEmptyHardState(rd.HardState) {
					require.NoError(t, m.s.SetHardState(rd.HardState))
				}
				require.NoError(t, m.s.Append(rd.Entries))
				msgs = append(msgs, rd.Messages...)
				for _, e := range rd.CommittedEntries {
					m.committed[e.GetIndex()] = e
					if e.GetType() == raftpb.EntryConfChange {
						cc := &raftpb.ConfChange{}
						require.NoError(t, proto.Unmarshal(e.GetData(), cc))
						m.rn.ApplyConfChange(cc)
					}
				}
				m.rn.Advance(rd)
			}
			for _, msg := range msgs {
				if side[msg.GetFrom()] == side[msg.GetTo()] {
					_ = ms[msg.GetTo()].rn.Step(msg)
				}
			}
		}
	}

	// Node 3 leads term 1 with voters 1, 2, 3; index 1 is committed everywhere.
	require.NoError(t, ms[3].rn.Campaign())
	stabilize()
	require.Equal(t, raft.StateLeader, ms[3].rn.Status().RaftState)
	require.Equal(t, uint64(1), ms[1].rn.Status().GetCommit())

	// With node 1 cut off, node 3 adds voter 4 (index 2), then voter 5 (index 3).
	side[1] = 1
	for _, id := range []uint64{4, 5} {
		cc := &raftpb.ConfChange{Type: raftpb.ConfChangeAddNode.Enum(), NodeId: new(id)}
		require.NoError(t, ms[3].rn.ProposeConfChange(cc))
		stabilize()
	}
	for id := uint64(2); id <= 5; id++ {
		require.Equal(t, uint64(3), ms[id].rn.Status().GetCommit(), "node %d", id)
	}

	// Node 1 is reconnected and receives indexes 2 and 3 with commit index 3 in one Ready;
	// it crashes in the Save, after the entry records and before the state record.
	side[1] = 0
	crash = true
	ms[3].rn.Tick() // heartbeat
	stabilize()
	require.False(t, crash, "node 1 did not crash")
	require.Equal(t, uint64(3), crashed.Entries[len(crashed.Entries)-1].GetIndex())
	require.Equal(t, uint64(3), crashed.HardState.GetCommit(), "the crashed Ready carried commit 3")

	// Partition {1, 2} | {3, 4, 5}. Node 3 commits y at index 4 with 4 and 5.
	side[1], side[2] = 1, 1
	require.NoError(t, ms[3].rn.Propose([]byte("y")))
	stabilize()
	require.Contains(t, ms[3].committed, uint64(4))

	// Node 2's election timeout elapses; node 1 campaigns in its configuration.
	for i := 0; i < 20; i++ {
		ms[2].rn.Tick()
		stabilize()
	}
	require.NoError(t, ms[1].rn.Campaign())
	stabilize()

	for idx, e := range ms[1].committed {
		if e3, ok := ms[3].committed[idx]; ok {
			assert.Equal(t, raft.DescribeEntry(e3, nil), raft.DescribeEntry(e, nil), "index %d committed on nodes 3 and 1", idx)
		}
	}
	assert.NotContains(t, ms[1].committed, uint64(4), "node 1 committed index 4 in its old configuration")

	// What node 1's WAL reads back is its storage.
	require.NoError(t, w.Close())
	var ents []*raftpb.Entry
	w, _, ents = readAllAt0(t, dir)
	last, err := ms[1].s.LastIndex()
	require.NoError(t, err)
	stored, err := ms[1].s.Entries(1, last+1, math.MaxUint64)
	require.NoError(t, err)
	assert.Equal(t, describe(stored), describe(ents))
}
