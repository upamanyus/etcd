// Copyright 2026 The etcd Authors
// SPDX-License-Identifier: Apache-2.0

package etcdserver

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"go.etcd.io/etcd/pkg/v3/wait"
	"go.etcd.io/etcd/server/v3/etcdserver/cindex"
	"go.etcd.io/etcd/server/v3/lease"
	betesting "go.etcd.io/etcd/server/v3/storage/backend/testing"
	"go.etcd.io/etcd/server/v3/storage/mvcc"
	"go.etcd.io/etcd/server/v3/storage/schema"
	"go.etcd.io/raft/v3/raftpb"
)

// TestApplySnapshotBelowBackend checks that a leader snapshot at or below the
// backend's consistent index (possible after a crash left the backend ahead of
// the WAL) keeps the backend and only moves the applied state to the snapshot.
func TestApplySnapshotBelowBackend(t *testing.T) {
	for _, idx := range []uint64{5, 7} {
		lg := zaptest.NewLogger(t)
		be, _ := betesting.NewDefaultTmpBackend(t)
		defer betesting.Close(t, be)
		tx := be.BatchTx()
		tx.LockOutsideApply()
		schema.UnsafeCreateMetaBucket(tx)
		schema.UnsafeUpdateConsistentIndex(tx, 7, 2)
		tx.Unlock()
		be.ForceCommit()
		ci := cindex.NewConsistentIndex(be)
		s := &EtcdServer{
			lgMu: new(sync.RWMutex), lg: lg, be: be, consistIndex: ci,
			applyWait: wait.NewTimeList(), stopping: make(chan struct{}),
		}
		s.kv = mvcc.New(lg, be, &lease.FakeLessor{}, mvcc.StoreConfig{})
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
