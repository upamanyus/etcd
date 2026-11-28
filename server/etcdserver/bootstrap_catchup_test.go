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
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"google.golang.org/protobuf/proto"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/pkg/v3/pbutil"
	"go.etcd.io/etcd/server/v3/config"
	"go.etcd.io/etcd/server/v3/etcdserver/api/membership"
	"go.etcd.io/etcd/server/v3/features"
	serverstorage "go.etcd.io/etcd/server/v3/storage"
	"go.etcd.io/etcd/server/v3/storage/schema"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// catchUpImage is a member's disk after a crash: a WAL (its entries from index 1, then its hard
// state) and a backend at consistent index ci, with term ciTerm, the member's raft term raftTerm
// saved with index raftTermIndex (ci if 0; none if raftTerm is 0), and the given voters as members.
type catchUpImage struct {
	walTerms      []uint64 // the term of WAL entry i+1
	walHS         *raftpb.HardState
	ci            uint64
	ciTerm        uint64
	raftTerm      uint64
	raftTermIndex uint64
	members       []uint64
}

// catchUpResult is the raft state bootstrap hands raft.
type catchUpResult struct {
	snapIndex, snapTerm uint64
	snapVoters          []uint64
	hs                  *raftpb.HardState
	entIndex            []uint64
	entTerm             []uint64
	members             map[uint64]string // member id -> name, the cluster in memory
}

func catchUpCfg(t *testing.T, dir string, forceNewCluster bool) config.ServerConfig {
	lg := zaptest.NewLogger(t)
	return config.ServerConfig{
		Name:              "m1",
		DataDir:           dir,
		Logger:            lg,
		TickMs:            10,
		ElectionTicks:     10,
		MaxRequestBytes:   1.5 * 1024 * 1024,
		ForceNewCluster:   forceNewCluster,
		ServerFeatureGate: features.NewDefaultServerFeatureGate("test", lg),
	}
}

func catchUpMember(id uint64) *membership.Member {
	return &membership.Member{
		ID:             types.ID(id),
		RaftAttributes: membership.RaftAttributes{PeerURLs: []string{fmt.Sprintf("http://m%d:2380", id)}},
		Attributes:     membership.Attributes{Name: fmt.Sprintf("m%d-at-ci", id)},
	}
}

func catchUpEntry(t *testing.T, i, term uint64) *raftpb.Entry {
	req := &etcdserverpb.InternalRaftRequest{Put: &etcdserverpb.PutRequest{Key: []byte(fmt.Sprintf("k%d", i)), Value: []byte(fmt.Sprint(term))}}
	data, err := proto.Marshal(req)
	require.NoError(t, err)
	return &raftpb.Entry{Type: raftpb.EntryType_EntryNormal.Enum(), Index: new(i), Term: new(term), Data: data}
}

func writeCatchUpImage(t *testing.T, cfg config.ServerConfig, img catchUpImage) {
	require.NoError(t, os.MkdirAll(cfg.SnapDir(), 0o700))
	md := pbutil.MustMarshalMessage(&etcdserverpb.Metadata{NodeID: new(uint64(1)), ClusterID: new(uint64(0x1000))})
	w, err := wal.Create(cfg.Logger, cfg.WALDir(), md)
	require.NoError(t, err)
	var ents []*raftpb.Entry
	for i, term := range img.walTerms {
		ents = append(ents, catchUpEntry(t, uint64(i+1), term))
	}
	require.NoError(t, w.Save(img.walHS, ents))
	require.NoError(t, w.Close())

	be := serverstorage.OpenBackend(cfg, nil)
	mbe := schema.NewMembershipBackend(cfg.Logger, be)
	mbe.MustCreateBackendBuckets()
	for _, id := range img.members {
		mbe.MustSaveMemberToBackend(catchUpMember(id))
	}
	tx := be.BatchTx()
	tx.LockOutsideApply()
	schema.UnsafeCreateMetaBucket(tx)
	schema.UnsafeUpdateConsistentIndex(tx, img.ci, img.ciTerm)
	if img.raftTerm != 0 {
		// The meta key "consistentIndexRaftTerm": the index and the raft term, big-endian.
		idx := img.raftTermIndex
		if idx == 0 {
			idx = img.ci
		}
		v := make([]byte, 16)
		binary.BigEndian.PutUint64(v[:8], idx)
		binary.BigEndian.PutUint64(v[8:], img.raftTerm)
		tx.UnsafePut(schema.Meta, []byte("consistentIndexRaftTerm"), v)
	}
	tx.Unlock()
	require.NoError(t, be.Close())
}

// bootCatchUp runs bootstrap on cfg's data dir and returns the raft state it hands raft, closing
// what it opened unless keep is set (then the caller gets the bootstrapped server to close).
func bootCatchUp(t *testing.T, cfg config.ServerConfig, keep bool) (catchUpResult, *bootstrappedServer) {
	b, err := bootstrap(cfg)
	require.NoError(t, err)
	var r catchUpResult
	sn := b.storage.wal.snapshot
	r.snapIndex, r.snapTerm = sn.GetMetadata().GetIndex(), sn.GetMetadata().GetTerm()
	r.snapVoters = sortedIDs(sn.GetMetadata().GetConfState().GetVoters())
	r.hs = proto.Clone(b.storage.wal.st).(*raftpb.HardState)
	for _, e := range b.storage.wal.ents {
		r.entIndex = append(r.entIndex, e.GetIndex())
		r.entTerm = append(r.entTerm, e.GetTerm())
	}
	r.members = map[uint64]string{}
	for _, m := range b.cluster.cl.Members() {
		r.members[uint64(m.ID)] = m.Name
	}
	if keep {
		return r, b
	}
	require.NoError(t, b.storage.wal.w.Close())
	b.Close()
	return r, nil
}

func sortedIDs(v []uint64) []uint64 {
	v = append([]uint64(nil), v...)
	sort.Slice(v, func(i, j int) bool { return v[i] < v[j] })
	return v
}

func walTail(t *testing.T, cfg config.ServerConfig) string {
	names, err := filepath.Glob(filepath.Join(cfg.WALDir(), "*.wal"))
	require.NoError(t, err)
	sort.Strings(names)
	return names[len(names)-1]
}

// walRecordEnds returns the offsets, past off, at which the records written from off end.
func walRecordEnds(b []byte, off int) []int {
	var ends []int
	for off+8 <= len(b) {
		lenField := binary.LittleEndian.Uint64(b[off:])
		if lenField == 0 {
			break
		}
		n := int(lenField & ^(uint64(0xff) << 56))
		if lenField&(uint64(0x80)<<56) != 0 {
			n += int((lenField >> 56) & 0x7)
		}
		off += 8 + n
		ends = append(ends, off)
	}
	return ends
}

// expectedCatchUp is what bootstrap must hand raft for img, for each crash point of its own write.
type catchUpExpect struct {
	snapIndex, snapTerm uint64
	snapVoters          []uint64
	term, vote, commit  uint64
	entIndex            []uint64
}

func checkCatchUp(t *testing.T, what string, r catchUpResult, e catchUpExpect, img catchUpImage) {
	require.Equal(t, e.snapIndex, r.snapIndex, "%s: snapshot index", what)
	require.Equal(t, e.snapTerm, r.snapTerm, "%s: snapshot term", what)
	// bootstrap gives raft the backend's configuration whatever the snapshot is.
	require.Equal(t, e.snapVoters, r.snapVoters, "%s: snapshot voters", what)
	require.Equal(t, e.term, r.hs.GetTerm(), "%s: hard state term", what)
	require.Equal(t, e.vote, r.hs.GetVote(), "%s: hard state vote", what)
	require.Equal(t, e.commit, r.hs.GetCommit(), "%s: hard state commit", what)
	require.Equal(t, e.entIndex, r.entIndex, "%s: log entries", what)
	// The cluster in memory is the backend's members at ci, with their attributes.
	require.Len(t, r.members, len(img.members), "%s: members", what)
	for _, id := range img.members {
		require.Equal(t, fmt.Sprintf("m%d-at-ci", id), r.members[id], "%s: member %d", what, id)
	}
}

// runCatchUpCase writes img, bootstraps it, checks the result against e, then checks every crash
// point of bootstrap's own write (each prefix of the records it appended to the WAL, whole
// records: the WAL's crash model, sector-atomic writes of 8-byte-aligned frames): the next
// bootstrap gives the same result, and a bootstrap after a complete one writes nothing
// (idempotent).
func runCatchUpCase(t *testing.T, img catchUpImage, e catchUpExpect) {
	pre := t.TempDir()
	cfg := catchUpCfg(t, pre, false)
	writeCatchUpImage(t, cfg, img)
	preTail, err := os.ReadFile(walTail(t, cfg))
	require.NoError(t, err)

	done := t.TempDir()
	copyTree(t, pre, done)
	dcfg := catchUpCfg(t, done, false)
	r, _ := bootCatchUp(t, dcfg, false)
	checkCatchUp(t, "first start", r, e, img)
	tailPath := walTail(t, dcfg)
	after, err := os.ReadFile(tailPath)
	require.NoError(t, err)

	// Idempotent: a second start finds the WAL caught up and writes nothing.
	r2, _ := bootCatchUp(t, dcfg, false)
	checkCatchUp(t, "second start", r2, e, img)
	again, err := os.ReadFile(tailPath)
	require.NoError(t, err)
	require.True(t, bytes.Equal(after, again), "the second start wrote to the WAL")

	// Crash points: the first start's records, cut before the first and after each one.
	first := 0
	for first < len(preTail) && first < len(after) && preTail[first] == after[first] {
		first++
	}
	first &^= 7
	ends := walRecordEnds(after, first)
	t.Logf("bootstrap appended %d records at offset %d (record ends %v)", len(ends), first, ends)
	cuts := append([]int{first}, ends...)
	for _, cut := range cuts {
		dir := t.TempDir()
		copyTree(t, pre, dir)
		ccfg := catchUpCfg(t, dir, false)
		crashed := append(append([]byte(nil), after[:cut]...), make([]byte, len(after)-cut)...)
		require.NoError(t, os.WriteFile(filepath.Join(ccfg.WALDir(), filepath.Base(tailPath)), crashed, 0o600))
		what := fmt.Sprintf("crash at offset %d", cut)
		rc, _ := bootCatchUp(t, ccfg, false)
		checkCatchUp(t, what, rc, e, img)
		rc2, _ := bootCatchUp(t, ccfg, false)
		checkCatchUp(t, what+", restarted again", rc2, e, img)
	}
}

// TestCatchUpWALBehindBackend: the WAL ends below ci (the upstream repro's state: a Ready's
// committed entries applied and committed to the backend, the crash before its Save). The start
// writes a snapshot record at ci, with the backend's term and members, and raises the commit index
// and the term (the vote goes with the old term).
func TestCatchUpWALBehindBackend(t *testing.T) {
	img := catchUpImage{
		walTerms: []uint64{1, 1, 1},
		walHS:    &raftpb.HardState{Term: new(uint64(1)), Vote: new(uint64(2)), Commit: new(uint64(3))},
		ci:       7, ciTerm: 2, raftTerm: 2,
		members: []uint64{1, 2, 3, 4, 5},
	}
	runCatchUpCase(t, img, catchUpExpect{
		snapIndex: 7, snapTerm: 2, snapVoters: []uint64{1, 2, 3, 4, 5},
		term: 2, vote: 0, commit: 7,
	})
}

// TestCatchUpWALCommitBehindBackend: the WAL holds the entry at ci with the backend's term, but its
// commit index is below ci (a commit-only hard state is not synced). Only the commit index is
// raised: the entries above ci, which this member may have acknowledged, stay.
func TestCatchUpWALCommitBehindBackend(t *testing.T) {
	img := catchUpImage{
		walTerms: []uint64{1, 1, 1, 2, 2, 2, 2, 2},
		walHS:    &raftpb.HardState{Term: new(uint64(2)), Vote: new(uint64(1)), Commit: new(uint64(3))},
		ci:       6, ciTerm: 2, raftTerm: 2,
		members: []uint64{1, 2, 3},
	}
	runCatchUpCase(t, img, catchUpExpect{
		snapIndex: 0, snapTerm: 0, snapVoters: []uint64{1, 2, 3},
		term: 2, vote: 1, commit: 6,
		entIndex: []uint64{1, 2, 3, 4, 5, 6, 7, 8},
	})
}

// TestCatchUpWALConflictsWithBackend: the WAL holds, at ci and after it, entries of an old term
// that the leader's log replaced (this member applied the replacing entry at ci, and the crash
// came before the Save that would have replaced them in the WAL). They are uncommitted (an entry
// of another term at ci is committed), so the snapshot at ci drops them; keeping them would put an
// entry of term 1 after the snapshot's term 2.
func TestCatchUpWALConflictsWithBackend(t *testing.T) {
	img := catchUpImage{
		walTerms: []uint64{1, 1, 1, 1, 1, 1, 1, 1},
		walHS:    &raftpb.HardState{Term: new(uint64(1)), Vote: new(uint64(3)), Commit: new(uint64(4))},
		ci:       6, ciTerm: 2, raftTerm: 2,
		members: []uint64{1, 2, 3},
	}
	runCatchUpCase(t, img, catchUpExpect{
		snapIndex: 6, snapTerm: 2, snapVoters: []uint64{1, 2, 3},
		term: 2, vote: 0, commit: 6,
	})
}

// TestCatchUpKeepsHigherTerm: the WAL holds the entry at ci and its term is above the backend's
// term for ci (the member voted in a later term before the crash): only the commit index is
// raised; the term and the vote stay.
func TestCatchUpKeepsHigherTerm(t *testing.T) {
	img := catchUpImage{
		walTerms: []uint64{1, 1, 1, 2, 2, 2, 2},
		walHS:    &raftpb.HardState{Term: new(uint64(5)), Vote: new(uint64(3)), Commit: new(uint64(3))},
		ci:       6, ciTerm: 2, raftTerm: 2,
		members: []uint64{1, 2, 3},
	}
	runCatchUpCase(t, img, catchUpExpect{
		snapIndex: 0, snapTerm: 0, snapVoters: []uint64{1, 2, 3},
		term: 5, vote: 3, commit: 6,
		entIndex: []uint64{1, 2, 3, 4, 5, 6, 7},
	})
}

// TestCatchUpSnapshotRaisesTerm: when the snapshot record replaces the log and the WAL's vote is
// the member's own (id 1: it campaigned at Term_w with the log the record drops), the term is
// raised above the WAL's even when the WAL's is at or above the backend's term for ci and its raft
// term: to Term_w + 1, with no vote. A member whose vote is another's, or none, keeps its term and
// vote (unless the raft term or ci's term is above it, as before). The second start finds the
// commit index at ci and changes nothing, and every crash point of the first start's write gives
// the same state (runCatchUpCase).
func TestCatchUpSnapshotRaisesTerm(t *testing.T) {
	hs := func(term, vote uint64) *raftpb.HardState {
		return &raftpb.HardState{Term: new(term), Vote: new(vote), Commit: new(uint64(3))}
	}
	for _, c := range []struct {
		name       string
		walTerms   []uint64
		walHS      *raftpb.HardState
		raftTerm   uint64
		term, vote uint64
	}{
		// Own vote: raised.
		{"own vote, WAL ends below ci", []uint64{1, 1, 1}, hs(5, 1), 2, 6, 0},
		{"own vote, WAL conflicts at ci", []uint64{1, 1, 1, 5, 5, 5, 5, 5}, hs(5, 1), 5, 6, 0},
		{"own vote, raft term equals the WAL's", []uint64{1, 1, 1}, hs(4, 1), 4, 5, 0},
		{"own vote, raft term above", []uint64{1, 1, 1}, hs(4, 1), 9, 9, 0},
		// Another's vote, or none: kept.
		{"other's vote, WAL ends below ci", []uint64{1, 1, 1}, hs(5, 3), 2, 5, 3},
		{"other's vote, WAL conflicts at ci", []uint64{1, 1, 1, 5, 5, 5, 5, 5}, hs(5, 2), 5, 5, 2},
		{"no vote, raft term equals the WAL's", []uint64{1, 1, 1}, hs(4, 0), 4, 4, 0},
		{"other's vote, raft term above", []uint64{1, 1, 1}, hs(4, 3), 9, 9, 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			img := catchUpImage{
				walTerms: c.walTerms, walHS: c.walHS,
				ci: 7, ciTerm: 2, raftTerm: c.raftTerm,
				members: []uint64{1, 2, 3},
			}
			runCatchUpCase(t, img, catchUpExpect{
				snapIndex: 7, snapTerm: 2, snapVoters: []uint64{1, 2, 3},
				term: c.term, vote: c.vote, commit: 7,
			})
		})
	}
}

// TestCatchUpNoTermRefused: a backend ahead of the WAL that has no term for ci (written before
// v3.5) cannot label a snapshot at ci: the start fails instead of guessing.
func TestCatchUpNoTermRefused(t *testing.T) {
	cfg := catchUpCfg(t, t.TempDir(), false)
	writeCatchUpImage(t, cfg, catchUpImage{
		walTerms: []uint64{1, 1, 1},
		walHS:    &raftpb.HardState{Term: new(uint64(1)), Commit: new(uint64(3))},
		ci:       7, ciTerm: 0,
		members: []uint64{1, 2, 3},
	})
	_, err := bootstrap(cfg)
	require.ErrorContains(t, err, "has no term")
}

// TestCatchUpRaftRefusesSnapshotBelowBackend is the keep guard's configuration case
// (etcd-grove nonissues/etcd-keepguard-raft-confstate-stale.md) on this design: the backend is at
// ci = 7 with members 1..5 (entries 6 and 7 added 4 and 5), the WAL at 3. The start brings raft to
// ci with voters 1..5; the leader's snapshot at 5 (voters 1..3) is refused by raft, which acks
// its commit index; the leader's entry 8 is appended after 7; raft's voters stay 1..5.
func TestCatchUpRaftRefusesSnapshotBelowBackend(t *testing.T) {
	cfg := catchUpCfg(t, t.TempDir(), false)
	writeCatchUpImage(t, cfg, catchUpImage{
		walTerms: []uint64{1, 1, 1},
		walHS:    &raftpb.HardState{Term: new(uint64(1)), Commit: new(uint64(3))},
		ci:       7, ciTerm: 2, raftTerm: 2,
		members: []uint64{1, 2, 3, 4, 5},
	})
	_, b := bootCatchUp(t, cfg, true)
	defer b.Close()
	defer b.storage.wal.w.Close()
	n := raft.RestartNode(b.raft.config)
	defer n.Stop()
	ms := b.raft.storage
	require.Equal(t, []uint64{1, 2, 3, 4, 5}, voters(t, n), "raft's configuration after the start")

	snap5 := &raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{Index: new(uint64(5)), Term: new(uint64(2)),
		ConfState: &raftpb.ConfState{Voters: []uint64{1, 2, 3}}}}
	require.NoError(t, n.Step(context.TODO(), &raftpb.Message{
		Type: raftpb.MessageType_MsgSnap.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(2)), Snapshot: snap5,
	}))
	rd := readyOnce(t, n, ms, nil)
	require.True(t, raft.IsEmptySnap(rd.Snapshot), "raft accepted a snapshot below the backend")
	var ack *raftpb.Message
	for _, m := range rd.Messages {
		if m.GetType() == raftpb.MessageType_MsgAppResp {
			ack = m
		}
	}
	require.NotNil(t, ack, "raft did not answer the snapshot")
	require.Equal(t, uint64(7), ack.GetIndex(), "raft acks its commit index")
	require.Equal(t, []uint64{1, 2, 3, 4, 5}, voters(t, n), "raft's configuration after the snapshot")

	require.NoError(t, n.Step(context.TODO(), &raftpb.Message{
		Type: raftpb.MessageType_MsgApp.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(2)),
		LogTerm: new(uint64(2)), Index: new(uint64(7)), Commit: new(uint64(8)),
		Entries: []*raftpb.Entry{catchUpEntry(t, 8, 2)},
	}))
	var applied []uint64
	readyOnce(t, n, ms, func(es []*raftpb.Entry) {
		for _, e := range es {
			applied = append(applied, e.GetIndex())
		}
	})
	require.Equal(t, []uint64{8}, applied, "raft hands out only the entries above the backend")
	require.Equal(t, []uint64{1, 2, 3, 4, 5}, voters(t, n))
}

// TestCatchUpForceNewCluster is etcd-io/etcd#22646 on this design: --force-new-cluster on a member
// whose backend is ahead of its WAL. Upstream sets the commit index to ci, past the WAL's last
// index, and raft panics; here the WAL is brought to ci first, and the forced conf changes follow
// ci.
func TestCatchUpForceNewCluster(t *testing.T) {
	cfg := catchUpCfg(t, t.TempDir(), true)
	writeCatchUpImage(t, cfg, catchUpImage{
		walTerms: []uint64{1, 1, 1},
		walHS:    &raftpb.HardState{Term: new(uint64(1)), Commit: new(uint64(3))},
		ci:       7, ciTerm: 2, raftTerm: 2,
		members: []uint64{1, 2, 3},
	})
	_, b := bootCatchUp(t, cfg, true)
	defer b.Close()
	defer b.storage.wal.w.Close()
	var idx []uint64
	for _, e := range b.storage.wal.ents {
		idx = append(idx, e.GetIndex())
	}
	require.Equal(t, []uint64{8, 9}, idx, "the forced removals of 2 and 3 follow ci")
	require.Equal(t, uint64(9), b.storage.wal.st.GetCommit())
	// raft starts (newRaft checks the commit index against the log).
	n := raft.RestartNode(b.raft.config)
	defer n.Stop()
	var committed []uint64
	readyOnce(t, n, b.raft.storage, func(es []*raftpb.Entry) {
		for _, e := range es {
			committed = append(committed, e.GetIndex())
		}
	})
	require.Equal(t, []uint64{8, 9}, committed, "raft hands out the forced conf changes")
}

// voters and readyOnce as in the keep guard's configuration test.
func voters(t *testing.T, n raft.Node) []uint64 {
	st := n.Status()
	var vs []uint64
	for id := range st.Config.Voters.IDs() {
		vs = append(vs, id)
	}
	return sortedIDs(vs)
}

func readyOnce(t *testing.T, n raft.Node, ms *raft.MemoryStorage, apply func([]*raftpb.Entry)) raft.Ready {
	rd := <-n.Ready()
	if !raft.IsEmptySnap(rd.Snapshot) {
		require.NoError(t, ms.ApplySnapshot(rd.Snapshot))
	}
	if !raft.IsEmptyHardState(rd.HardState) {
		require.NoError(t, ms.SetHardState(rd.HardState))
	}
	require.NoError(t, ms.Append(rd.Entries))
	if len(rd.CommittedEntries) > 0 && apply != nil {
		apply(rd.CommittedEntries)
	}
	n.Advance()
	return rd
}
