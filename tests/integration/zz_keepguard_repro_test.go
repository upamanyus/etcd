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

package integration

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/etcdserver"
	"go.etcd.io/etcd/tests/v3/framework/integration"
	"go.etcd.io/raft/v3/raftpb"
)

// TestReproBackendAheadOfWALSnapshotRegress: a follower F applies committed
// entries that its raft loop has not yet written to the WAL, its backend commits
// them, and it crashes (we take a copy of its data dir at that moment = crash
// image). The leader A's apply loop lags (paused). F restarts from the crash
// image; A, whose raft log is compacted past F's WAL, sends a snapshot at its
// applied index k with F's WAL last index < k < F's consistent index. Upstream
// applySnapshot replaces F's backend by that older snapshot and F's serializable
// Range revision goes back.
func TestReproBackendAheadOfWALSnapshotRegress(t *testing.T) {
	integration.BeforeTest(t)
	if os.Getenv("REPRO_KEEP_VERIFY") == "" {
		// BeforeTest turns on ETCD_VERIFY; its "consistent index not advancing"
		// check trips when the old backend is closed (see report). Turn it off
		// to observe the client-visible behaviour.
		t.Setenv("ETCD_VERIFY", "")
	}
	clus := integration.NewCluster(t, &integration.ClusterConfig{Size: 3, SnapshotCount: 10000, SnapshotCatchUpEntries: 1})
	defer clus.Terminate(t)
	defer etcdserver.SetReproHooks(nil)

	li := clus.WaitLeader(t)
	A := clus.Members[li]
	fi, bi := (li+1)%3, (li+2)%3
	F, B := clus.Members[fi], clus.Members[bi]
	cB, cF := clus.Client(bi), clus.Client(fi)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		_, err := cB.Put(ctx, "warm", fmt.Sprint(i))
		require.NoError(t, err)
	}
	time.Sleep(500 * time.Millisecond)

	var mu sync.Mutex
	holdA := make(chan struct{})    // phase 1: A's apply held at start of applyAll
	phase1 := true                  // guarded by mu
	var P uint64                    // phase 2 threshold (0 = unarmed)
	phase2Done := false
	var kSent uint64
	fBlocked := make(chan [2]uint64, 1) // (first unstable index, last committed index)
	releaseF := make(chan struct{})
	fArmed := false
	afterSnap := make(chan uint64, 1)
	releaseSnap := make(chan struct{})
	aRaft := A.Server.ReproRaftNode()
	_ = aRaft
	fRaft := F.Server.ReproRaftNode()
	fID := F.Server.MemberID()

	etcdserver.SetReproHooks(&etcdserver.ReproHooks{
		BeforeSave: func(r any, ents, committed []*raftpb.Entry) {
			mu.Lock()
			armed := fArmed && r == fRaft
			mu.Unlock()
			if !armed || len(ents) == 0 || len(committed) == 0 {
				return
			}
			if committed[len(committed)-1].GetIndex() < ents[0].GetIndex() {
				return
			}
			mu.Lock()
			fArmed = false
			mu.Unlock()
			fBlocked <- [2]uint64{ents[0].GetIndex(), committed[len(committed)-1].GetIndex()}
			<-releaseF
		},
		ApplyAll: func(s *etcdserver.EtcdServer, stage string, appliedi uint64) {
			if s != A.Server {
				return
			}
			mu.Lock()
			p1, p := phase1, P
			mu.Unlock()
			switch {
			case stage == "start" && p1:
				<-holdA
			case stage == "beforeSnapshot" && p != 0 && appliedi >= p && !phase2Done:
				s.ForceSnapshot()
			case stage == "beforeMsgSnap" && p != 0 && appliedi >= p && !phase2Done:
				mu.Lock()
				phase2Done = true
				kSent = appliedi
				mu.Unlock()
				t.Logf("A holds at applied %d (snapshot+compaction done) until a MsgSnap is requested", appliedi)
				deadline := time.Now().Add(30 * time.Second)
				for s.ReproPendingMsgSnap() == 0 && time.Now().Before(deadline) {
					time.Sleep(5 * time.Millisecond)
				}
				t.Logf("A: pending MsgSnap=%d, sending snapshot at %d", s.ReproPendingMsgSnap(), appliedi)
			}
		},
		AfterApplySnapshot: func(s *etcdserver.EtcdServer, index uint64) {
			if s.MemberID() != fID {
				return
			}
			afterSnap <- index
			<-releaseSnap
		},
	})

	// Phase 1: hold A's apply, cut F off, commit N entries through B.
	F.InjectPartition(t, A, B)
	const N = 20
	var lastRev int64
	for i := 0; i < N; i++ {
		resp, err := cB.Put(ctx, "k", fmt.Sprint(i))
		require.NoError(t, err)
		lastRev = resp.Header.Revision
	}
	t.Logf("committed %d puts via B; last revision %d; A applied index %d", N, lastRev, A.Server.ReproAppliedIndex())

	// F catches up in one MsgApp whose commit covers the new entries: they are
	// applied before the raft loop writes them to the WAL; block that write.
	mu.Lock()
	fArmed = true
	mu.Unlock()
	F.RecoverPartition(t, A, B)
	var fb [2]uint64
	select {
	case fb = <-fBlocked:
	case <-time.After(10 * time.Second):
		t.Fatal("F never got a Ready whose committed entries overlap its unstable entries")
	}
	w := fb[0] - 1
	t.Logf("F raft loop blocked before WAL Save: unstable from %d, committed to %d (WAL last index w=%d)", fb[0], fb[1], w)
	var ci uint64
	require.Eventually(t, func() bool { ci = F.Server.ReproConsistentIndex(); return ci >= fb[1] }, 10*time.Second, 5*time.Millisecond)
	gr, err := cF.Get(ctx, "k", clientv3.WithSerializable())
	require.NoError(t, err)
	rNew := gr.Header.Revision
	t.Logf("F consistent index %d > w=%d; serializable Get at F: revision R_new=%d value=%s", ci, w, rNew, gr.Kvs[0].Value)
	F.Server.Backend().ForceCommit() // what the 100ms batch timer does anyway
	time.Sleep(300 * time.Millisecond)

	image := filepath.Join(t.TempDir(), "crash-image")
	require.NoError(t, os.CopyFS(image, os.DirFS(F.DataDir)))
	t.Logf("crash image of F taken (backend ci=%d, WAL last index %d)", ci, w)

	// Crash F: cut it off first so nothing it does after the image (its WAL write
	// and the MsgAppResp sent after it) reaches the others, then let its
	// goroutines finish, stop it, and put the crash image in place.
	F.InjectPartition(t, A, B)
	close(releaseF)
	F.Stop(t)
	require.NoError(t, os.RemoveAll(F.DataDir))
	require.NoError(t, os.CopyFS(F.DataDir, os.DirFS(image)))

	// Phase 2: A's apply resumes; at P it snapshots and compacts its raft log past
	// F's WAL, then waits for a MsgSnap request before handling it.
	mu.Lock()
	P = w + 3
	phase1 = false
	mu.Unlock()
	close(holdA)
	require.Eventually(t, func() bool { mu.Lock(); defer mu.Unlock(); return phase2Done }, 10*time.Second, 5*time.Millisecond)
	mu.Lock()
	k := kSent
	mu.Unlock()
	t.Logf("A will send a snapshot at k=%d (w=%d < k, ci=%d)", k, w, ci)
	require.Less(t, k, ci, "scenario needs k < F's consistent index")

	require.NoError(t, F.Restart(t))
	F.RecoverPartition(t, A, B)
	var snapIdx uint64
	select {
	case snapIdx = <-afterSnap:
	case <-time.After(30 * time.Second):
		t.Fatal("F never applied a leader snapshot")
	}
	ready := false
	select {
	case <-F.Server.ReadyNotify():
		ready = true
	default:
	}
	kvRev := F.Server.KV().Rev()
	gctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	gr2, err := cF.Get(gctx, "k", clientv3.WithSerializable())
	cancel()
	close(releaseSnap)
	require.NoError(t, err)
	rOld := gr2.Header.Revision
	t.Logf("F applied leader snapshot at %d; F ready (published)=%v; F mvcc Rev()=%d; serializable Get at F: revision R_old=%d value=%s",
		snapIdx, ready, kvRev, rOld, gr2.Kvs[0].Value)
	require.Less(t, rOld, rNew, "expected F's revision to regress")

	// F catches up again.
	require.Eventually(t, func() bool {
		r, err := cF.Get(ctx, "k", clientv3.WithSerializable())
		return err == nil && r.Header.Revision >= rNew
	}, 20*time.Second, 20*time.Millisecond)
	t.Logf("F caught up to revision >= R_new again")
}

// TestKeepFixDRestartAtConsistentIndex is TestReproBackendAheadOfWALSnapshotRegress on the fix
// (design D): F restarts with its raft log brought up to its backend, so A never sends it a
// snapshot at or below ci, and F never serves a revision below R_new.
func TestKeepFixDRestartAtConsistentIndex(t *testing.T) {
	integration.BeforeTest(t)
	// ETCD_VERIFY stays on (BeforeTest): upstream panics here with "update of
	// consistent index not advancing".
	clus := integration.NewCluster(t, &integration.ClusterConfig{Size: 3, SnapshotCount: 10000, SnapshotCatchUpEntries: 1})
	defer clus.Terminate(t)
	defer etcdserver.SetReproHooks(nil)

	li := clus.WaitLeader(t)
	A := clus.Members[li]
	fi, bi := (li+1)%3, (li+2)%3
	F, B := clus.Members[fi], clus.Members[bi]
	cB, cF := clus.Client(bi), clus.Client(fi)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		_, err := cB.Put(ctx, "warm", fmt.Sprint(i))
		require.NoError(t, err)
	}
	time.Sleep(500 * time.Millisecond)

	var mu sync.Mutex
	holdA := make(chan struct{})    // phase 1: A's apply held at start of applyAll
	phase1 := true                  // guarded by mu
	var P uint64                    // phase 2 threshold (0 = unarmed)
	phase2Done := false
	var kSent uint64
	fBlocked := make(chan [2]uint64, 1) // (first unstable index, last committed index)
	releaseF := make(chan struct{})
	fArmed := false
	afterSnap := make(chan uint64, 1)
	releaseSnap := make(chan struct{})
	aRaft := A.Server.ReproRaftNode()
	_ = aRaft
	fRaft := F.Server.ReproRaftNode()
	fID := F.Server.MemberID()

	etcdserver.SetReproHooks(&etcdserver.ReproHooks{
		BeforeSave: func(r any, ents, committed []*raftpb.Entry) {
			mu.Lock()
			armed := fArmed && r == fRaft
			mu.Unlock()
			if !armed || len(ents) == 0 || len(committed) == 0 {
				return
			}
			if committed[len(committed)-1].GetIndex() < ents[0].GetIndex() {
				return
			}
			mu.Lock()
			fArmed = false
			mu.Unlock()
			fBlocked <- [2]uint64{ents[0].GetIndex(), committed[len(committed)-1].GetIndex()}
			<-releaseF
		},
		ApplyAll: func(s *etcdserver.EtcdServer, stage string, appliedi uint64) {
			if s != A.Server {
				return
			}
			mu.Lock()
			p1, p := phase1, P
			mu.Unlock()
			switch {
			case stage == "start" && p1:
				<-holdA
			case stage == "beforeSnapshot" && p != 0 && appliedi >= p && !phase2Done:
				s.ForceSnapshot()
			case stage == "beforeMsgSnap" && p != 0 && appliedi >= p && !phase2Done:
				mu.Lock()
				phase2Done = true
				kSent = appliedi
				mu.Unlock()
				t.Logf("A holds at applied %d (snapshot+compaction done) until a MsgSnap is requested", appliedi)
				deadline := time.Now().Add(3 * time.Second)
				for s.ReproPendingMsgSnap() == 0 && time.Now().Before(deadline) {
					time.Sleep(5 * time.Millisecond)
				}
				t.Logf("A: pending MsgSnap=%d, sending snapshot at %d", s.ReproPendingMsgSnap(), appliedi)
			}
		},
		AfterApplySnapshot: func(s *etcdserver.EtcdServer, index uint64) {
			if s.MemberID() != fID {
				return
			}
			afterSnap <- index
			<-releaseSnap
		},
	})

	// Phase 1: hold A's apply, cut F off, commit N entries through B.
	F.InjectPartition(t, A, B)
	const N = 20
	var lastRev int64
	for i := 0; i < N; i++ {
		resp, err := cB.Put(ctx, "k", fmt.Sprint(i))
		require.NoError(t, err)
		lastRev = resp.Header.Revision
	}
	t.Logf("committed %d puts via B; last revision %d; A applied index %d", N, lastRev, A.Server.ReproAppliedIndex())

	// F catches up in one MsgApp whose commit covers the new entries: they are
	// applied before the raft loop writes them to the WAL; block that write.
	mu.Lock()
	fArmed = true
	mu.Unlock()
	F.RecoverPartition(t, A, B)
	var fb [2]uint64
	select {
	case fb = <-fBlocked:
	case <-time.After(10 * time.Second):
		t.Fatal("F never got a Ready whose committed entries overlap its unstable entries")
	}
	w := fb[0] - 1
	t.Logf("F raft loop blocked before WAL Save: unstable from %d, committed to %d (WAL last index w=%d)", fb[0], fb[1], w)
	var ci uint64
	require.Eventually(t, func() bool { ci = F.Server.ReproConsistentIndex(); return ci >= fb[1] }, 10*time.Second, 5*time.Millisecond)
	gr, err := cF.Get(ctx, "k", clientv3.WithSerializable())
	require.NoError(t, err)
	rNew := gr.Header.Revision
	t.Logf("F consistent index %d > w=%d; serializable Get at F: revision R_new=%d value=%s", ci, w, rNew, gr.Kvs[0].Value)
	F.Server.Backend().ForceCommit() // what the 100ms batch timer does anyway
	time.Sleep(300 * time.Millisecond)

	image := filepath.Join(t.TempDir(), "crash-image")
	require.NoError(t, os.CopyFS(image, os.DirFS(F.DataDir)))
	t.Logf("crash image of F taken (backend ci=%d, WAL last index %d)", ci, w)

	// Crash F: cut it off first so nothing it does after the image (its WAL write
	// and the MsgAppResp sent after it) reaches the others, then let its
	// goroutines finish, stop it, and put the crash image in place.
	F.InjectPartition(t, A, B)
	close(releaseF)
	F.Stop(t)
	require.NoError(t, os.RemoveAll(F.DataDir))
	require.NoError(t, os.CopyFS(F.DataDir, os.DirFS(image)))

	// Phase 2: A's apply resumes; at P it snapshots and compacts its raft log past
	// F's WAL, then waits for a MsgSnap request before handling it.
	mu.Lock()
	P = w + 3
	phase1 = false
	mu.Unlock()
	close(holdA)
	require.Eventually(t, func() bool { mu.Lock(); defer mu.Unlock(); return phase2Done }, 10*time.Second, 5*time.Millisecond)
	mu.Lock()
	k := kSent
	mu.Unlock()
	t.Logf("A will send a snapshot at k=%d (w=%d < k, ci=%d)", k, w, ci)
	require.Less(t, k, ci, "scenario needs k < F's consistent index")

	require.NoError(t, F.Restart(t))
	// Bootstrap started raft at F's consistent index: its commit index is ci already.
	require.GreaterOrEqual(t, F.Server.ReproRaftCommit(), ci, "raft's commit index after the restart")
	var mu2 sync.Mutex
	minRev := int64(-1)
	pollCtx, stopPoll := context.WithCancel(ctx)
	defer stopPoll()
	pollDone := make(chan struct{})
	go func() {
		defer close(pollDone)
		for pollCtx.Err() == nil {
			gctx, cancel := context.WithTimeout(pollCtx, 200*time.Millisecond)
			r, gerr := cF.Get(gctx, "k", clientv3.WithSerializable())
			cancel()
			if gerr == nil {
				mu2.Lock()
				if minRev < 0 || r.Header.Revision < minRev {
					minRev = r.Header.Revision
				}
				mu2.Unlock()
			}
			time.Sleep(time.Millisecond)
		}
	}()
	F.RecoverPartition(t, A, B)
	var installed []uint64
	collectDone := make(chan struct{})
	defer close(collectDone)
	go func() {
		for {
			select {
			case idx := <-afterSnap:
				mu2.Lock()
				installed = append(installed, idx)
				mu2.Unlock()
				releaseSnap <- struct{}{}
			case <-collectDone:
				return
			}
		}
	}()
	// F catches up past ci from A's log (A compacted only below k <= ci).
	require.Eventually(t, func() bool {
		r, err := cF.Get(ctx, "k", clientv3.WithSerializable())
		return err == nil && r.Header.Revision >= rNew
	}, 20*time.Second, 20*time.Millisecond)
	_, err = cB.Put(ctx, "after", "x")
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		r, err := cF.Get(ctx, "after", clientv3.WithSerializable())
		return err == nil && len(r.Kvs) == 1
	}, 20*time.Second, 20*time.Millisecond)
	stopPoll()
	<-pollDone
	mu2.Lock()
	defer mu2.Unlock()
	t.Logf("F restarted: raft commit %d (ci=%d, k=%d), voters %v; snapshots installed %v; lowest revision F served %d (R_new=%d)",
		F.Server.ReproRaftCommit(), ci, k, F.Server.ReproRaftVoters(), installed, minRev, rNew)
	for _, idx := range installed {
		require.Greater(t, idx, ci, "F installed a snapshot at or below its consistent index")
	}
	require.GreaterOrEqual(t, minRev, rNew, "F served a revision below the one its backend held before the crash")
	require.Len(t, F.Server.ReproRaftVoters(), 3)
}
