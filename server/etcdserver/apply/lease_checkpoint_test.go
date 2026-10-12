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

package apply

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"golang.org/x/crypto/bcrypt"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/server/v3/auth"
	"go.etcd.io/etcd/server/v3/etcdserver/api/membership"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3alarm"
	"go.etcd.io/etcd/server/v3/etcdserver/cindex"
	"go.etcd.io/etcd/server/v3/lease"
	"go.etcd.io/etcd/server/v3/storage/backend"
	betesting "go.etcd.io/etcd/server/v3/storage/backend/testing"
	"go.etcd.io/etcd/server/v3/storage/mvcc"
	"go.etcd.io/etcd/server/v3/storage/schema"
)

// newCheckpointPersistApplier returns an UberApplier on be whose lessor persists checkpoints
// (as with a cluster version >= 3.6), and an apply function that applies a request as the raft
// entry of the given index.
func newCheckpointPersistApplier(t *testing.T, be backend.Backend, ci cindex.ConsistentIndexer) func(index uint64, r *pb.InternalRaftRequest) *Result {
	lg := zaptest.NewLogger(t)
	cluster := membership.NewCluster(lg)
	cluster.SetBackend(schema.NewMembershipBackend(lg, be))
	cluster.AddMember(&membership.Member{ID: memberID}, true)
	lessor := lease.NewLessor(lg, be, cluster, lease.LessorConfig{CheckpointPersist: true})
	kv := mvcc.NewStore(lg, be, lessor, mvcc.StoreConfig{})
	alarmStore, err := v3alarm.NewAlarmStore(lg, schema.NewAlarmBackend(lg, be))
	require.NoError(t, err)
	tp, err := auth.NewTokenProvider(lg, "simple", dummyIndexWaiter, 300*time.Second)
	require.NoError(t, err)
	authStore := auth.NewAuthStore(lg, schema.NewAuthBackend(lg, be), tp, bcrypt.DefaultCost)
	ua := NewUberApplier(ApplierOptions{
		Logger:               lg,
		KV:                   kv,
		AlarmStore:           alarmStore,
		AuthStore:            authStore,
		Lessor:               lessor,
		Cluster:              cluster,
		RaftStatus:           &fakeRaftStatusGetter{},
		SnapshotServer:       &fakeSnapshotServer{},
		ConsistentIndex:      ci,
		Backend:              be,
		QuotaBackendBytesCfg: 16 * 1024 * 1024, // 16MB
		WarningApplyDuration: time.Hour,
	})
	return func(index uint64, r *pb.InternalRaftRequest) *Result {
		ci.SetConsistentApplyingIndex(index, 1)
		r.Header = &pb.RequestHeader{ID: index}
		return ua.Apply(&InternalRaftRequestWrapper{InternalRaftRequest: r}, membership.ApplyBoth)
	}
}

func checkpointRequest(ids []int64, remainingTTL int64) *pb.InternalRaftRequest {
	var cps []*pb.LeaseCheckpoint
	for _, id := range ids {
		cps = append(cps, &pb.LeaseCheckpoint{ID: id, Remaining_TTL: remainingTTL})
	}
	return &pb.InternalRaftRequest{LeaseCheckpoint: &pb.LeaseCheckpointRequest{Checkpoints: cps}}
}

// TestLeaseCheckpointOneCriticalSection checks that applying a LeaseCheckpoint request takes the
// backend's batch lock inside apply once, whatever the number of its checkpoints: the
// consistent index set by the post-lock hook is then committed with all of the request's writes.
func TestLeaseCheckpointOneCriticalSection(t *testing.T) {
	for _, n := range []int{1, 2, 10} {
		t.Run(fmt.Sprintf("checkpoints=%d", n), func(t *testing.T) {
			be, _ := betesting.NewDefaultTmpBackend(t)
			t.Cleanup(func() { betesting.Close(t, be) })
			apply := newCheckpointPersistApplier(t, be, cindex.NewConsistentIndex(be))

			var ids []int64
			index := uint64(0)
			for id := int64(1); id <= int64(n); id++ {
				index++
				res := apply(index, &pb.InternalRaftRequest{LeaseGrant: &pb.LeaseGrantRequest{ID: id, TTL: 600}})
				require.NoError(t, res.Err)
				ids = append(ids, id)
			}

			sections := 0
			be.SetTxPostLockInsideApplyHook(func() { sections++ })
			index++
			res := apply(index, checkpointRequest(ids, 100))
			require.NoError(t, res.Err)
			require.Equalf(t, 1, sections, "a LeaseCheckpoint request with %d checkpoints took the batch lock inside apply %d times", n, sections)
		})
	}
}

// TestLeaseCheckpointCrashConsistency checks that a backend commit made while a LeaseCheckpoint
// request is applied cannot persist the request's consistent index without all of its
// checkpoints. The backend commits at every Unlock with pending writes (batch limit 1); the
// post-lock hook sets the consistent index as the server's does, and copies the database file
// each time, which is what a crash at that point leaves on disk.
func TestLeaseCheckpointCrashConsistency(t *testing.T) {
	lg := zaptest.NewLogger(t)
	var ci cindex.ConsistentIndexer
	bcfg := backend.DefaultBackendConfig(lg)
	bcfg.BatchLimit = 1
	bcfg.Hooks = backend.NewHooks(func(tx backend.UnsafeReadWriter) { ci.UnsafeSave(tx) })
	be, dbPath := betesting.NewTmpBackendFromCfg(t, bcfg)
	t.Cleanup(func() { betesting.Close(t, be) })
	ci = cindex.NewConsistentIndex(be)
	apply := newCheckpointPersistApplier(t, be, ci)

	ids := []int64{1, 2, 3}
	index := uint64(0)
	for _, id := range ids {
		index++
		res := apply(index, &pb.InternalRaftRequest{LeaseGrant: &pb.LeaseGrantRequest{ID: id, TTL: 600}})
		require.NoError(t, res.Err)
	}

	var crashes []string
	be.SetTxPostLockInsideApplyHook(func() {
		// the server's hook (EtcdServer.getTxPostLockInsideApplyHook)
		applyingIdx, applyingTerm := ci.ConsistentApplyingIndex()
		if applyingIdx > ci.UnsafeConsistentIndex() {
			ci.SetConsistentIndex(applyingIdx, applyingTerm)
		}
		// the batch lock is held: the file holds the last commit
		data, err := os.ReadFile(dbPath)
		require.NoError(t, err)
		crash := filepath.Join(t.TempDir(), "crash.db")
		require.NoError(t, os.WriteFile(crash, data, 0o600))
		crashes = append(crashes, crash)
	})
	index++
	checkpointIndex := index
	res := apply(checkpointIndex, checkpointRequest(ids, 100))
	require.NoError(t, res.Err)
	index++
	res = apply(index, &pb.InternalRaftRequest{Put: &pb.PutRequest{Key: []byte("k"), Value: []byte("v")}})
	require.NoError(t, res.Err)

	checked := 0
	for i, crash := range crashes {
		cbe := backend.NewDefaultBackend(lg, crash)
		persistedIndex, _ := schema.ReadConsistentIndex(cbe.ReadTx())
		if persistedIndex >= checkpointIndex {
			checked++
			// on restart the checkpoint entry is not applied again: its checkpoints must be there
			tx := cbe.ReadTx()
			tx.RLock()
			for _, id := range ids {
				l := schema.MustUnsafeGetLease(tx, id)
				require.NotNil(t, l)
				require.Equalf(t, int64(100), l.RemainingTTL,
					"crash %d: consistent index %d persisted, lease %d's checkpoint is not", i, persistedIndex, id)
			}
			tx.RUnlock()
		}
		require.NoError(t, cbe.Close())
	}
	require.Positivef(t, checked, "no crash point after the checkpoint entry was committed")
}
