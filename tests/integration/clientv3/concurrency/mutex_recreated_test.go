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

package concurrency_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.etcd.io/etcd/tests/v3/framework/integration"
)

const (
	lockReturnTimeout = 10 * time.Second // bound for a Lock that must return
	lockBlockedPeriod = 2 * time.Second  // how long a Lock that must block is watched
)

// TestMutexLockSharedSessionKeyRecreated checks that Lock does not return
// while another session holds the lock, when the waiting Mutex's key is
// deleted and recreated by another Mutex on the same session and prefix.
//
// Mutexes A and B on session S share the key pfx/<lease of S>. With C (on
// session T) holding the lock, A and B wait behind it, both with A's key at
// revision r1. A's Lock is canceled, which deletes the shared key. D (on
// session U) then waits, with a key at r2 > r1, and A locks again, recreating
// the shared key at r3 > r2. When C unlocks, D is the first-created key and
// holds the lock; B must keep waiting until D unlocks.
func TestMutexLockSharedSessionKeyRecreated(t *testing.T) {
	testMutexLockKeyRecreated(t, "/recreated-lock", true)
}

// TestMutexLockSeparateSessionsKeyNotRecreated runs the same schedule with B
// on its own session, so that A's cancellation does not touch B's key.
func TestMutexLockSeparateSessionsKeyNotRecreated(t *testing.T) {
	testMutexLockKeyRecreated(t, "/recreated-lock-control", false)
}

func testMutexLockKeyRecreated(t *testing.T, pfx string, shared bool) {
	cli, err := integration.NewClient(t, clientv3.Config{Endpoints: exampleEndpoints()})
	require.NoError(t, err)
	t.Cleanup(func() { cli.Close() })

	// A and B use cliS, whose completed Txn RPCs signal that a Lock's
	// tryAcquire has run (B's does not change any key when it shares A's).
	cliS, txnDone := newTxnSignallingClient(t)

	sS := newSession(t, cliS)
	sB := sS
	if !shared {
		sB = newSession(t, cliS)
	}
	sT := newSession(t, cli)
	sU := newSession(t, cli)

	mA := concurrency.NewMutex(sS, pfx)
	mB := concurrency.NewMutex(sB, pfx)
	mC := concurrency.NewMutex(sT, pfx)
	mD := concurrency.NewMutex(sU, pfx)
	keyA, keyB, keyD := mutexKey(pfx, sS), mutexKey(pfx, sB), mutexKey(pfx, sU)

	require.NoError(t, mC.Lock(t.Context()))

	ctxA, cancelA := context.WithCancel(t.Context())
	a1 := lockAsync(ctxA, mA)
	waitTxn(t, txnDone)
	r1 := waitKeyCreated(t, cli, keyA)

	b := lockAsync(t.Context(), mB)
	waitTxn(t, txnDone)
	rB := waitKeyCreated(t, cli, keyB)

	// Cancel A's Lock: it deletes A's key (shared with B if shared).
	cancelA()
	require.Error(t, a1.wait(t))
	waitKeyDeleted(t, cli, keyA)

	d := lockAsync(t.Context(), mD)
	r2 := waitKeyCreated(t, cli, keyD)

	ctxA2, cancelA2 := context.WithCancel(t.Context())
	defer cancelA2()
	a2 := lockAsync(ctxA2, mA)
	waitTxn(t, txnDone)
	r3 := waitKeyCreated(t, cli, keyA)
	require.Less(t, r1, r2)
	require.Less(t, r2, r3)

	require.NoError(t, mC.Unlock(t.Context()))

	if shared {
		// D's key (r2) is now first-created; the shared key is at r3.
		require.NoError(t, d.wait(t))
		require.Equal(t, keyD, firstCreatedKey(t, cli, pfx))
		require.True(t, isOwner(t, cli, mD))
		b.requireBlocked(t)
		a2.requireBlocked(t)

		require.NoError(t, mD.Unlock(t.Context()))
		require.NoError(t, b.wait(t))
		require.NoError(t, a2.wait(t))
		require.Equal(t, keyB, firstCreatedKey(t, cli, pfx))
		require.True(t, isOwner(t, cli, mB))
		require.True(t, isOwner(t, cli, mA))
		require.NoError(t, mB.Unlock(t.Context()))
		return
	}

	// B's key (rB < r2) is first-created; D waits until B unlocks.
	require.Less(t, rB, r2)
	require.NoError(t, b.wait(t))
	require.Equal(t, keyB, firstCreatedKey(t, cli, pfx))
	require.True(t, isOwner(t, cli, mB))
	d.requireBlocked(t)

	require.NoError(t, mB.Unlock(t.Context()))
	require.NoError(t, d.wait(t))
	require.True(t, isOwner(t, cli, mD))

	cancelA2()
	require.Error(t, a2.wait(t))
}

// newTxnSignallingClient returns a client that sends on the returned channel
// after each completed Txn RPC.
func newTxnSignallingClient(t *testing.T) (*clientv3.Client, chan struct{}) {
	t.Helper()
	txnDone := make(chan struct{}, 64)
	interceptor := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		err := invoker(ctx, method, req, reply, cc, opts...)
		if method == "/etcdserverpb.KV/Txn" {
			txnDone <- struct{}{}
		}
		return err
	}
	cli, err := integration.NewClient(t, clientv3.Config{
		Endpoints:   exampleEndpoints(),
		DialOptions: []grpc.DialOption{grpc.WithChainUnaryInterceptor(interceptor)},
	})
	require.NoError(t, err)
	t.Cleanup(func() { cli.Close() })
	return cli, txnDone
}

func newSession(t *testing.T, cli *clientv3.Client) *concurrency.Session {
	t.Helper()
	s, err := concurrency.NewSession(cli)
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })
	return s
}

func mutexKey(pfx string, s *concurrency.Session) string {
	return fmt.Sprintf("%s/%x", pfx, s.Lease())
}

type asyncLock struct {
	done chan struct{}
	err  error
}

func lockAsync(ctx context.Context, m *concurrency.Mutex) *asyncLock {
	l := &asyncLock{done: make(chan struct{})}
	go func() {
		defer close(l.done)
		l.err = m.Lock(ctx)
	}()
	return l
}

func (l *asyncLock) wait(t *testing.T) error {
	t.Helper()
	select {
	case <-l.done:
		return l.err
	case <-time.After(lockReturnTimeout):
		t.Fatalf("Lock did not return within %v", lockReturnTimeout)
		return nil
	}
}

func (l *asyncLock) requireBlocked(t *testing.T) {
	t.Helper()
	select {
	case <-l.done:
		t.Fatalf("Lock returned (err=%v) while another session holds the lock", l.err)
	case <-time.After(lockBlockedPeriod):
	}
}

func waitTxn(t *testing.T, txnDone chan struct{}) {
	t.Helper()
	select {
	case <-txnDone:
	case <-time.After(lockReturnTimeout):
		t.Fatalf("no Txn completed within %v", lockReturnTimeout)
	}
}

// waitKeyCreated waits until key exists and returns its create revision.
func waitKeyCreated(t *testing.T, cli *clientv3.Client, key string) int64 {
	t.Helper()
	var rev int64
	require.Eventually(t, func() bool {
		resp, err := cli.Get(t.Context(), key)
		if err != nil || len(resp.Kvs) == 0 {
			return false
		}
		rev = resp.Kvs[0].CreateRevision
		return true
	}, lockReturnTimeout, 10*time.Millisecond)
	return rev
}

func waitKeyDeleted(t *testing.T, cli *clientv3.Client, key string) {
	t.Helper()
	require.Eventually(t, func() bool {
		resp, err := cli.Get(t.Context(), key)
		return err == nil && len(resp.Kvs) == 0
	}, lockReturnTimeout, 10*time.Millisecond)
}

// firstCreatedKey returns the lock holder under pfx: the key with the
// smallest create revision.
func firstCreatedKey(t *testing.T, cli *clientv3.Client, pfx string) string {
	t.Helper()
	resp, err := cli.Get(t.Context(), pfx, clientv3.WithFirstCreate()...)
	require.NoError(t, err)
	require.NotEmpty(t, resp.Kvs)
	return string(resp.Kvs[0].Key)
}

func isOwner(t *testing.T, cli *clientv3.Client, m *concurrency.Mutex) bool {
	t.Helper()
	resp, err := cli.Txn(t.Context()).If(m.IsOwner()).Commit()
	require.NoError(t, err)
	return resp.Succeeded
}
