// Copyright 2026 The etcd Authors
// SPDX-License-Identifier: Apache-2.0

package etcdserver

import (
	"sync"
	"testing"

	"github.com/Masterminds/semver/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"

	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/pkg/v3/wait"
	"go.etcd.io/etcd/server/v3/etcdserver/api"
	"go.etcd.io/etcd/server/v3/etcdserver/api/membership"
	"go.etcd.io/etcd/server/v3/etcdserver/cindex"
	"go.etcd.io/etcd/server/v3/lease"
	"go.etcd.io/etcd/server/v3/mock/mockstorage"
	"go.etcd.io/etcd/server/v3/storage/backend"
	betesting "go.etcd.io/etcd/server/v3/storage/backend/testing"
	"go.etcd.io/etcd/server/v3/storage/mvcc"
	"go.etcd.io/etcd/server/v3/storage/schema"
	"go.etcd.io/raft/v3/raftpb"
)

// newBelowBackendServer returns a server over be, whose consistent index is 7,
// with the cluster cl (recovered from be) and the transport tr.
func newBelowBackendServer(t *testing.T, lg *zap.Logger, be backend.Backend, cl *membership.RaftCluster, tr *peerTransporter) *EtcdServer {
	tx := be.BatchTx()
	tx.LockOutsideApply()
	schema.UnsafeCreateMetaBucket(tx)
	schema.UnsafeUpdateConsistentIndex(tx, 7, 2)
	tx.Unlock()
	be.ForceCommit()
	cl.Recover(api.UpdateCapability)
	ci := cindex.NewConsistentIndex(be)
	r := newRaftNode(raftNodeConfig{lg: lg, transport: tr, storage: mockstorage.NewStorageRecorder(t.TempDir())})
	s := &EtcdServer{
		lgMu: new(sync.RWMutex), lg: lg, be: be, consistIndex: ci, r: *r, cluster: cl, memberID: 1,
		applyWait: wait.NewTimeList(), stopping: make(chan struct{}),
	}
	s.kv = mvcc.New(lg, be, &lease.FakeLessor{}, mvcc.StoreConfig{})
	return s
}

// TestApplySnapshotBelowBackend checks that a leader snapshot at or below the
// backend's consistent index (possible after a crash left the backend ahead of
// the WAL) keeps the backend and only moves the applied state to the snapshot.
func TestApplySnapshotBelowBackend(t *testing.T) {
	for _, idx := range []uint64{5, 7} {
		lg := zaptest.NewLogger(t)
		be, _ := betesting.NewDefaultTmpBackend(t)
		defer betesting.Close(t, be)
		cl := membership.NewCluster(lg)
		cl.SetBackend(schema.NewMembershipBackend(lg, be))
		s := newBelowBackendServer(t, lg, be, cl, newPeerTransporter())
		defer s.kv.Close()

		done := make(chan struct{})
		close(done)
		cs := &raftpb.ConfState{Voters: []uint64{1, 2, 3}}
		ep := etcdProgress{appliedi: 3, appliedt: 1, diskSnapshotIndex: 3, memorySnapshotIndex: 3}
		snapshot := &raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{Index: new(idx), Term: new(uint64(2)), ConfState: cs}}
		// Without the guard this would open the snapshot's database, which this
		// server (no snapshotter) cannot: the call returning shows the guard taken.
		s.applySnapshot(&ep, &toApply{snapshot: snapshot, notifyc: done})

		require.Same(t, be, s.be, "the backend was replaced")
		require.Equal(t, uint64(7), s.consistIndex.ConsistentIndex(), "the backend moved back")
		require.Equal(t, idx, ep.appliedi)
		require.Equal(t, uint64(2), ep.appliedt)
		require.Equal(t, idx, ep.diskSnapshotIndex)
		require.Equal(t, idx, ep.memorySnapshotIndex)
		require.Same(t, cs, ep.confState)
	}
}

// TestApplySnapshotBelowBackendRecoversCluster checks that a snapshot kept below
// the backend recovers the cluster configuration and the transport's peers from
// the backend. A restart replays the entries at or below the backend's consistent
// index on the cluster in memory only, so that an older member attribute, peer
// URL or cluster version replayed there stays until a later entry sets it again;
// when the snapshot covers those later entries the apply loop never sees them.
func TestApplySnapshotBelowBackendRecoversCluster(t *testing.T) {
	lg := zaptest.NewLogger(t)
	be, _ := betesting.NewDefaultTmpBackend(t)
	defer betesting.Close(t, be)
	mbe := schema.NewMembershipBackend(lg, be)
	cl := membership.NewCluster(lg)
	cl.SetBackend(mbe)

	// The backend, at consistent index 7: members 1 (this server) and 2, member 3
	// removed, cluster version 3.5.
	newURLs := []string{"http://new:2380"}
	oldURLs := []string{"http://old:2380"}
	m1 := &membership.Member{ID: 1, RaftAttributes: membership.RaftAttributes{PeerURLs: []string{"http://self:2380"}}, Attributes: membership.Attributes{Name: "m1"}}
	m2 := &membership.Member{ID: 2, RaftAttributes: membership.RaftAttributes{PeerURLs: newURLs}, Attributes: membership.Attributes{Name: "m2-new"}}
	mbe.MustSaveMemberToBackend(m1)
	mbe.MustSaveMemberToBackend(m2)
	mbe.MustDeleteMemberFromBackend(3)
	mbe.MustSaveClusterVersionToBackend(semver.MustParse("3.5.0"))
	tr := newPeerTransporter()
	s := newBelowBackendServer(t, lg, be, cl, tr)
	defer s.kv.Close()
	tr.AddPeer(2, newURLs)

	// The restart replay of entries below the consistent index (the WAL ends at 4):
	// member 3 added (removed at 6), member 2's older name and peer URLs, and an
	// older cluster version.
	tr.AddPeer(3, []string{"http://m3:2380"})
	cl.UpdateRaftAttributes(2, membership.RaftAttributes{PeerURLs: oldURLs}, membership.ApplyV2storeOnly)
	tr.UpdatePeer(2, oldURLs)
	cl.UpdateAttributes(2, membership.Attributes{Name: "m2-old"}, membership.ApplyV2storeOnly)
	cl.SetVersion(semver.MustParse("3.4.0"), api.UpdateCapability, membership.ApplyV2storeOnly)

	done := make(chan struct{})
	close(done)
	ep := etcdProgress{appliedi: 4, appliedt: 1, diskSnapshotIndex: 3, memorySnapshotIndex: 3}
	snapshot := &raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{
		Index: new(uint64(6)), Term: new(uint64(2)), ConfState: &raftpb.ConfState{Voters: []uint64{1, 2}},
	}}
	s.applySnapshot(&ep, &toApply{snapshot: snapshot, notifyc: done})

	require.Same(t, be, s.be, "the backend was replaced")
	assert.Equal(t, "m2-new", cl.Member(2).Name, "stale member attributes")
	assert.Equal(t, newURLs, cl.Member(2).PeerURLs, "stale member peer URLs")
	assert.Equal(t, "3.5.0", cl.Version().String(), "stale cluster version")
	assert.Equal(t, map[types.ID][]string{2: newURLs}, tr.peers, "stale transport peers")
}

// peerTransporter records the peers it is given.
type peerTransporter struct {
	nopTransporter
	peers map[types.ID][]string
}

func newPeerTransporter() *peerTransporter {
	return &peerTransporter{peers: make(map[types.ID][]string)}
}

func (s *peerTransporter) AddPeer(id types.ID, us []string)    { s.peers[id] = us }
func (s *peerTransporter) RemovePeer(id types.ID)              { delete(s.peers, id) }
func (s *peerTransporter) RemoveAllPeers()                     { clear(s.peers) }
func (s *peerTransporter) UpdatePeer(id types.ID, us []string) { s.peers[id] = us }
