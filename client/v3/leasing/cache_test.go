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

package leasing

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v3pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	v3 "go.etcd.io/etcd/client/v3"
)

func newTestLeaseKey(key string) *leaseKey {
	return &leaseKey{
		response: &v3.GetResponse{
			Header: &v3pb.ResponseHeader{Revision: 10},
			Kvs:    []*mvccpb.KeyValue{{Key: []byte(key), Value: []byte("v"), CreateRevision: 5, ModRevision: 5, Version: 1}},
			Count:  1,
		},
		rev:   10,
		waitc: closedCh,
	}
}

func TestLeaseCacheEvictRange(t *testing.T) {
	lc := &leaseCache{entries: make(map[string]*leaseKey), revokes: make(map[string]time.Time)}
	for _, k := range []string{"a", "b", "c", "d"} {
		lc.entries[k] = newTestLeaseKey(k)
	}

	lc.EvictRange("a", "c")

	for _, k := range []string{"a", "b"} {
		require.NotContainsf(t, lc.entries, k, "expected %q in [a, c) to be evicted", k)
		require.Containsf(t, lc.revokes, k, "expected %q in [a, c) to be marked revoked", k)
		require.Falsef(t, lc.MayAcquire(k), "expected %q in [a, c) not to be acquired again before the backoff", k)
	}
	for _, k := range []string{"c", "d"} {
		require.Containsf(t, lc.entries, k, "expected %q outside [a, c) to stay cached", k)
		require.NotContainsf(t, lc.revokes, k, "expected %q outside [a, c) not to be marked revoked", k)
	}
}

func TestLeaseCacheEvictRangeBeginNotCached(t *testing.T) {
	lc := &leaseCache{entries: make(map[string]*leaseKey), revokes: make(map[string]time.Time)}
	for _, k := range []string{"b", "d"} {
		lc.entries[k] = newTestLeaseKey(k)
	}

	// "a" is not cached; "b" is in [a, c) and must be evicted.
	lc.EvictRange("a", "c")

	require.NotContains(t, lc.entries, "b")
	require.Contains(t, lc.revokes, "b")
	require.NotContains(t, lc.revokes, "a")
	require.Contains(t, lc.entries, "d")
}

func TestLeaseCacheEvictRangeFromKey(t *testing.T) {
	lc := &leaseCache{entries: make(map[string]*leaseKey), revokes: make(map[string]time.Time)}
	for _, k := range []string{"a", "b", "c"} {
		lc.entries[k] = newTestLeaseKey(k)
	}

	// end "\x00" is WithFromKey: every key >= "b".
	lc.EvictRange("b", "\x00")

	require.Contains(t, lc.entries, "a")
	for _, k := range []string{"b", "c"} {
		require.NotContains(t, lc.entries, k)
		require.Contains(t, lc.revokes, k)
	}
}
