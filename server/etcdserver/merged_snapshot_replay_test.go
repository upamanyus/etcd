// Copyright 2026 The etcd Authors
// SPDX-License-Identifier: Apache-2.0

package etcdserver

import (
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"google.golang.org/protobuf/proto"

	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/pkg/v3/wait"
	"go.etcd.io/etcd/server/v3/config"
	"go.etcd.io/etcd/server/v3/etcdserver/api/membership"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/etcd/server/v3/etcdserver/cindex"
	"go.etcd.io/etcd/server/v3/lease"
	"go.etcd.io/etcd/server/v3/storage/backend"
	betesting "go.etcd.io/etcd/server/v3/storage/backend/testing"
	"go.etcd.io/etcd/server/v3/storage/mvcc"
	"go.etcd.io/etcd/server/v3/storage/schema"
	"go.etcd.io/raft/v3/raftpb"
)

type replaySnapTransport struct {
	nopTransporter
	send func(*snap.Message)
}

func (tr replaySnapTransport) SendSnapshot(m *snap.Message) { tr.send(m) }

// The member restarts with an applied backend at 7. Its apply loop has
// replayed only through 3 when it takes a current-term MsgSnap. Compare the
// label with the metadata in the database bytes delivered to the follower.
func TestMergedSnapshotDuringReplay(t *testing.T) {
	for _, tc := range []struct {
		name              string
		legacy, bootstrap bool
	}{
		{"stored metadata", false, false},
		{"pre-3.5 metadata", true, false},
		{"recovered current backend", false, true},
		{"recovered pre-3.5 backend", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) { testReplaySnapshot(t, tc.legacy, tc.bootstrap) })
	}
}

func testReplaySnapshot(t *testing.T, legacy, bootstrap bool) {
	lg := zaptest.NewLogger(t)
	be, path := betesting.NewDefaultTmpBackend(t)
	conf := &raftpb.ConfState{Voters: []uint64{1, 2, 3}, Learners: []uint64{4}}
	tx := be.BatchTx()
	tx.LockOutsideApply()
	schema.UnsafeCreateMetaBucket(tx)
	schema.UnsafeUpdateConsistentIndex(tx, 7, 2)
	if legacy {
		tx.UnsafeDelete(schema.Meta, schema.MetaTermKeyName)
	} else {
		schema.MustUnsafeSaveConfStateToBackend(lg, tx, conf)
	}
	tx.Unlock()
	if legacy || bootstrap {
		members := schema.NewMembershipBackend(lg, be)
		members.MustCreateBackendBuckets()
		for id := 1; id <= 4; id++ {
			members.MustSaveMemberToBackend(&membership.Member{ID: types.ID(id), RaftAttributes: membership.RaftAttributes{IsLearner: id == 4}})
		}
	}
	require.NoError(t, be.Close())

	be = backend.NewDefaultBackend(lg, path)
	defer be.Close()
	ci := cindex.NewConsistentIndex(be)
	require.Equal(t, uint64(7), ci.ConsistentIndex())
	sent := false
	tr := replaySnapTransport{send: func(m *snap.Message) {
		sent = true
		file := filepath.Join(t.TempDir(), "received.db")
		f, err := os.Create(file)
		require.NoError(t, err)
		_, err = io.Copy(f, m.ReadCloser)
		require.NoError(t, err)
		require.NoError(t, f.Close())
		m.CloseWithError(nil)
		received := backend.NewDefaultBackend(lg, file)
		defer received.Close()
		r := received.ReadTx()
		r.RLock()
		i, term := schema.UnsafeReadConsistentIndex(r)
		cs := schema.UnsafeConfStateFromBackend(lg, r)
		r.RUnlock()
		md := m.GetSnapshot().GetMetadata()
		require.Equal(t, uint64(7), i)
		if legacy {
			require.Zero(t, term)
			require.Nil(t, cs)
		} else {
			require.Equal(t, uint64(2), term)
			require.True(t, proto.Equal(conf, cs))
		}
		require.Equal(t, i, md.GetIndex())
		require.Equal(t, uint64(2), md.GetTerm())
		require.ElementsMatch(t, conf.Voters, md.GetConfState().Voters)
		require.ElementsMatch(t, conf.Learners, md.GetConfState().Learners)
	}}
	r := newRaftNode(raftNodeConfig{lg: lg, Node: newNopReadyNode(), transport: &tr})
	s := &EtcdServer{
		lgMu: new(sync.RWMutex), lg: lg, be: be, consistIndex: ci,
		r: *r, Cfg: config.ServerConfig{SnapshotCount: 1000},
		applyWait: wait.NewTimeList(), stopping: make(chan struct{}),
	}
	s.kv = mvcc.New(lg, be, &lease.FakeLessor{}, mvcc.StoreConfig{})
	defer s.kv.Close()
	defer func() { close(s.stopping); s.wg.Wait() }()
	done := make(chan struct{})
	close(done)
	s.r.msgSnapC <- &raftpb.Message{
		Type: raftpb.MsgSnap.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(3)),
		Snapshot: &raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{Index: new(uint64(3)), Term: new(uint64(1))}},
	}
	ep := etcdProgress{appliedi: 3, appliedt: 1, confState: &raftpb.ConfState{Voters: []uint64{1, 2, 3}}}
	if bootstrap {
		// This is the configuration bootstrap actually installs. The WAL
		// snapshot and the replay cursor can predate learner 4.
		conf := buildConfStateFromV3store(lg, be)
		ep.replayConf = &conf
	}
	// HardState.Commit can lag a committed backend across a power loss.
	bwal := &bootstrappedWAL{
		meta:     &snapshotMetadata{nodeID: 1},
		st:       &raftpb.HardState{Term: new(uint64(2)), Commit: new(uint64(3))},
		snapshot: &raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{Index: new(uint64(3)), Term: new(uint64(1))}},
	}
	for i := uint64(4); i <= 7; i++ {
		bwal.ents = append(bwal.ents, &raftpb.Entry{Index: new(i), Term: new(uint64(2))})
	}
	boot := bootstrapRaftFromWAL(config.ServerConfig{Logger: lg}, bwal)
	s.applyAll(&ep, &toApply{notifyc: done, raftAdvancedC: done, raftTerm: 3, replayTerms: boot.replayTerms})
	require.True(t, sent, "applyAll did not deliver the current-term snapshot")
}

func TestReplayTermsKeepEarlierBatch(t *testing.T) {
	entries := func(first uint64, terms ...uint64) []*raftpb.Entry {
		var ents []*raftpb.Entry
		for i, term := range terms {
			ents = append(ents, &raftpb.Entry{Index: new(first + uint64(i)), Term: new(term)})
		}
		return ents
	}
	old := replayTerms(nil).appendEntries(entries(4, 1, 2, 2, 2, 3))
	replaced := old.appendEntries(entries(6, 3, 3, 4))
	extended := replaced.appendEntries(entries(9, 4, 4)).after(5)
	require.Equal(t, uint64(2), old.termAt(7))
	require.Equal(t, uint64(3), old.termAt(8))
	require.Equal(t, uint64(2), replaced.termAt(5))
	require.Equal(t, uint64(3), replaced.termAt(7))
	require.Equal(t, uint64(4), extended.termAt(10))
	require.Len(t, extended, 2)
}
