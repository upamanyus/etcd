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

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.etcd.io/etcd/tests/v3/framework/integration"
)

const (
	campaignReturnTimeout = 10 * time.Second // bound for a Campaign that must return
	campaignBlockedPeriod = 2 * time.Second  // how long a Campaign that must block is watched
)

// The tests below check Campaign's promise that "only one can be the leader
// at a time" when the campaigner's key is deleted while Campaign waits for
// the keys created before it: once Campaign returns nil, its key must still
// exist with the create revision it recorded, Leader() must report that key,
// and Proclaim must succeed.

// TestElectionCampaignSharedSessionKeyDeleted checks that Campaign does not
// report leadership after its key was deleted by another Election on the
// same session and prefix while it waited.
//
// Elections A and B on session S share the key K = pfx/<lease of S>. With C
// (on session T) leading, A creates K and waits; B reuses K (proclaiming its
// own value) and waits. A's Campaign is canceled, which resigns, deleting K.
// D (on session U) then campaigns and waits. When C resigns, D is the leader;
// B's key is gone, so B's Campaign must return an error rather than nil.
func TestElectionCampaignSharedSessionKeyDeleted(t *testing.T) {
	pfx := "/election-campaign-shared"
	cli := newElectionClient(t)

	sS := newElectionSession(t, cli)
	sT := newElectionSession(t, cli)
	sU := newElectionSession(t, cli)

	eA := concurrency.NewElection(sS, pfx)
	eB := concurrency.NewElection(sS, pfx)
	eC := concurrency.NewElection(sT, pfx)
	eD := concurrency.NewElection(sU, pfx)
	keyK, keyC, keyD := electionKey(pfx, sS), electionKey(pfx, sT), electionKey(pfx, sU)

	require.NoError(t, eC.Campaign(t.Context(), "c"))
	require.Equal(t, keyC, electionLeaderKey(t, eC))

	ctxA, cancelA := context.WithCancel(t.Context())
	defer cancelA()
	a := campaignAsync(ctxA, eA, "a")
	rK := waitElectionKeyValue(t, cli, keyK, "a")

	// B finds K, proclaims "b" over A's value, then waits behind C.
	b := campaignAsync(t.Context(), eB, "b")
	require.Equal(t, rK, waitElectionKeyValue(t, cli, keyK, "b"))

	// Cancel A's Campaign: it resigns, deleting K, which B shares.
	cancelA()
	require.ErrorIs(t, a.wait(t), context.Canceled)
	waitElectionKeyDeleted(t, cli, keyK)

	d := campaignAsync(t.Context(), eD, "d")
	rD := waitElectionKeyValue(t, cli, keyD, "d")
	require.Less(t, rK, rD)

	b.requireBlocked(t)
	d.requireBlocked(t)

	require.NoError(t, eC.Resign(t.Context()))

	require.NoError(t, d.wait(t))
	require.Equal(t, keyD, electionLeaderKey(t, eD))

	requireCampaignLost(t, cli, "B", eB, b.wait(t))
}

// TestElectionCampaignLeaseRevoked checks that Campaign does not report
// leadership after its session's lease was revoked while it waited.
//
// With C (on session T) leading, A (on session S) campaigns and waits. S's
// lease is revoked, deleting A's key. D (on session U) then campaigns and
// waits. When C resigns, D is the leader; A's Campaign must return an error
// rather than nil. Lease expiry and Session.Close delete the key the same way.
func TestElectionCampaignLeaseRevoked(t *testing.T) {
	pfx := "/election-campaign-revoked"
	cli := newElectionClient(t)

	sS := newElectionSession(t, cli)
	sT := newElectionSession(t, cli)
	sU := newElectionSession(t, cli)

	eA := concurrency.NewElection(sS, pfx)
	eC := concurrency.NewElection(sT, pfx)
	eD := concurrency.NewElection(sU, pfx)
	keyA, keyD := electionKey(pfx, sS), electionKey(pfx, sU)

	require.NoError(t, eC.Campaign(t.Context(), "c"))

	a := campaignAsync(t.Context(), eA, "a")
	rA := waitElectionKeyValue(t, cli, keyA, "a")

	_, err := cli.Revoke(t.Context(), sS.Lease())
	require.NoError(t, err)
	waitElectionKeyDeleted(t, cli, keyA)

	d := campaignAsync(t.Context(), eD, "d")
	rD := waitElectionKeyValue(t, cli, keyD, "d")
	require.Less(t, rA, rD)

	a.requireBlocked(t)
	d.requireBlocked(t)

	require.NoError(t, eC.Resign(t.Context()))

	require.NoError(t, d.wait(t))
	require.Equal(t, keyD, electionLeaderKey(t, eD))

	requireCampaignLost(t, cli, "A", eA, a.wait(t))
}

// TestElectionCampaignSeparateSessionsKeyKept runs the shared-session
// schedule with B on its own session, so that A's cancellation does not
// touch B's key: B is the leader once C resigns, and D after B resigns. It
// checks that the fix does not make Campaign give up when its own key is
// intact.
func TestElectionCampaignSeparateSessionsKeyKept(t *testing.T) {
	pfx := "/election-campaign-control"
	cli := newElectionClient(t)

	sS := newElectionSession(t, cli)
	sB := newElectionSession(t, cli)
	sT := newElectionSession(t, cli)
	sU := newElectionSession(t, cli)

	eA := concurrency.NewElection(sS, pfx)
	eB := concurrency.NewElection(sB, pfx)
	eC := concurrency.NewElection(sT, pfx)
	eD := concurrency.NewElection(sU, pfx)
	keyA, keyB, keyD := electionKey(pfx, sS), electionKey(pfx, sB), electionKey(pfx, sU)

	require.NoError(t, eC.Campaign(t.Context(), "c"))

	ctxA, cancelA := context.WithCancel(t.Context())
	defer cancelA()
	a := campaignAsync(ctxA, eA, "a")
	waitElectionKeyValue(t, cli, keyA, "a")

	b := campaignAsync(t.Context(), eB, "b")
	rB := waitElectionKeyValue(t, cli, keyB, "b")

	cancelA()
	require.ErrorIs(t, a.wait(t), context.Canceled)
	waitElectionKeyDeleted(t, cli, keyA)

	d := campaignAsync(t.Context(), eD, "d")
	rD := waitElectionKeyValue(t, cli, keyD, "d")
	require.Less(t, rB, rD)

	b.requireBlocked(t)

	require.NoError(t, eC.Resign(t.Context()))

	requireCampaignLeader(t, cli, "B", eB, b.wait(t))
	d.requireBlocked(t)

	require.NoError(t, eB.Resign(t.Context()))
	requireCampaignLeader(t, cli, "D", eD, d.wait(t))
	require.NoError(t, eD.Resign(t.Context()))
}

// requireCampaignLost checks that a Campaign whose key was deleted while it
// waited returned ErrElectionNotLeader and left e without a leader key. If
// the Campaign returned nil, it reports how e fails to be the leader.
func requireCampaignLost(t *testing.T, cli *clientv3.Client, name string, e *concurrency.Election, err error) {
	t.Helper()
	if err == nil {
		requireCampaignLeader(t, cli, name, e, err)
		t.Fatalf("%s's Campaign returned nil after its key was deleted", name)
	}
	require.ErrorIsf(t, err, concurrency.ErrElectionNotLeader, "%s's Campaign", name)
	require.Emptyf(t, e.Key(), "%s's Key() after a lost Campaign", name)
	require.ErrorIsf(t, e.Proclaim(t.Context(), name+"-proclaimed"), concurrency.ErrElectionNotLeader, "%s's Proclaim after a lost Campaign", name)
}

// requireCampaignLeader checks that a Campaign that returned nil left e the
// leader: its key exists with the create revision it recorded, Leader()
// reports it, and Proclaim succeeds.
func requireCampaignLeader(t *testing.T, cli *clientv3.Client, name string, e *concurrency.Election, err error) {
	t.Helper()
	require.NoErrorf(t, err, "%s's Campaign", name)

	key, rev := e.Key(), e.Rev()
	resp, gerr := cli.Get(t.Context(), key)
	require.NoError(t, gerr)
	keyExists := len(resp.Kvs) == 1 && resp.Kvs[0].CreateRevision == rev
	leader, lerr := e.Leader(t.Context())
	leaderKey := ""
	if lerr == nil {
		leaderKey = string(leader.Kvs[0].Key)
	}
	perr := e.Proclaim(t.Context(), name+"-proclaimed")
	t.Logf("%s: Campaign returned nil; Key()=%s Rev()=%d; key exists at that revision: %v; Leader()=%q (err %v); Proclaim: %v",
		name, key, rev, keyExists, leaderKey, lerr, perr)

	require.Truef(t, keyExists, "%s's Campaign returned nil but its key %s (create revision %d) no longer exists", name, key, rev)
	require.NoErrorf(t, lerr, "%s's Campaign returned nil but Leader() fails", name)
	require.Equalf(t, key, leaderKey, "%s's Campaign returned nil but Leader() is another key", name)
	require.NoErrorf(t, perr, "%s's Campaign returned nil but Proclaim fails", name)
}

func newElectionClient(t *testing.T) *clientv3.Client {
	t.Helper()
	cli, err := integration.NewClient(t, clientv3.Config{Endpoints: exampleEndpoints()})
	require.NoError(t, err)
	t.Cleanup(func() { cli.Close() })
	return cli
}

func newElectionSession(t *testing.T, cli *clientv3.Client) *concurrency.Session {
	t.Helper()
	s, err := concurrency.NewSession(cli)
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })
	return s
}

func electionKey(pfx string, s *concurrency.Session) string {
	return fmt.Sprintf("%s/%x", pfx, s.Lease())
}

func electionLeaderKey(t *testing.T, e *concurrency.Election) string {
	t.Helper()
	resp, err := e.Leader(t.Context())
	require.NoError(t, err)
	return string(resp.Kvs[0].Key)
}

type asyncCampaign struct {
	done chan struct{}
	err  error
}

func campaignAsync(ctx context.Context, e *concurrency.Election, val string) *asyncCampaign {
	c := &asyncCampaign{done: make(chan struct{})}
	go func() {
		defer close(c.done)
		c.err = e.Campaign(ctx, val)
	}()
	return c
}

func (c *asyncCampaign) wait(t *testing.T) error {
	t.Helper()
	select {
	case <-c.done:
		return c.err
	case <-time.After(campaignReturnTimeout):
		t.Fatalf("Campaign did not return within %v", campaignReturnTimeout)
		return nil
	}
}

func (c *asyncCampaign) requireBlocked(t *testing.T) {
	t.Helper()
	select {
	case <-c.done:
		t.Fatalf("Campaign returned (err=%v) while another session leads", c.err)
	case <-time.After(campaignBlockedPeriod):
	}
}

// waitElectionKeyValue waits until key holds val and returns its create
// revision.
func waitElectionKeyValue(t *testing.T, cli *clientv3.Client, key, val string) int64 {
	t.Helper()
	var rev int64
	require.Eventually(t, func() bool {
		resp, err := cli.Get(t.Context(), key)
		if err != nil || len(resp.Kvs) == 0 || string(resp.Kvs[0].Value) != val {
			return false
		}
		rev = resp.Kvs[0].CreateRevision
		return true
	}, campaignReturnTimeout, 10*time.Millisecond)
	return rev
}

func waitElectionKeyDeleted(t *testing.T, cli *clientv3.Client, key string) {
	t.Helper()
	require.Eventually(t, func() bool {
		resp, err := cli.Get(t.Context(), key)
		return err == nil && len(resp.Kvs) == 0
	}, campaignReturnTimeout, 10*time.Millisecond)
}
