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

package watch

import (
	"context"
	"sync"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/framework/integration"
)

// A progress request followed by the creation of a watch from an old revision, on one stream:
// the server answers the progress request (all watchers synced, revision r) before it registers
// the new watch, but its sendLoop may send the new watch's Created response first; the client
// then broadcasts the progress notification to the new watch, whose events below r are still to
// come. Reported if the first response of the new watch is a progress notification.
func TestWatchProgressBeforeCreatedRace(t *testing.T) {
	integration.BeforeTest(t)
	clus := integration.NewCluster(t, &integration.ClusterConfig{Size: 1})
	defer clus.Terminate(t)
	cli := clus.Client(0)
	ctx := t.Context()
	for i := 0; i < 10; i++ {
		if _, err := cli.Put(ctx, "k", "v"); err != nil {
			t.Fatal(err)
		}
	}
	hits, trials := 0, 0
	for trial := 0; trial < 500 && hits == 0; trial++ {
		trials++
		tctx, cancel := context.WithCancel(ctx)
		bw := cli.Watch(tctx, "busy", clientv3.WithPrefix())
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 30; j++ {
				cli.Put(ctx, "busy/x", "v")
			}
		}()
		time.Sleep(time.Millisecond)
		if err := cli.RequestProgress(tctx); err != nil {
			t.Fatal(err)
		}
		wch := cli.Watch(tctx, "k", clientv3.WithRev(2), clientv3.WithProgressNotify())
		select {
		case resp := <-wch:
			if resp.IsProgressNotify() {
				hits++
				t.Logf("trial %d: progress notification at revision %d before the watch's events (from revision 2)",
					trial, resp.Header.Revision)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("no response on the new watch")
		}
		cancel()
		for range bw {
		}
		wg.Wait()
	}
	if hits > 0 {
		t.Fatalf("%d of %d trials: a progress notification reached a new watch before its events", hits, trials)
	}
	t.Logf("no hit in %d trials", trials)
}
