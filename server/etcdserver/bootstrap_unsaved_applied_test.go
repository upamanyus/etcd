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
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/server/v3/config"
	"go.etcd.io/etcd/server/v3/features"
	serverstorage "go.etcd.io/etcd/server/v3/storage"
	"go.etcd.io/etcd/server/v3/storage/schema"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/raft/v3/raftpb"
)

// holdFirstSave holds the raft loop's first Save (the bootstrap Ready's) until released: the
// state a crash before that Save reached the disk leaves.
type holdFirstSave struct {
	serverstorage.Storage
	entered chan struct{}
	release chan struct{}
	first   bool
}

func (h *holdFirstSave) Save(st *raftpb.HardState, ents []*raftpb.Entry) error {
	if !h.first {
		h.first = true
		close(h.entered)
		<-h.release
	}
	return h.Storage.Save(st, ents)
}

func copyTree(t *testing.T, src, dst string) {
	require.NoError(t, filepath.Walk(src, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, p)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, info.Mode().Perm())
		}
		in, err := os.Open(p)
		if err != nil {
			return err
		}
		defer in.Close()
		out, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, info.Mode().Perm())
		if err != nil {
			return err
		}
		defer out.Close()
		_, err = io.Copy(out, in)
		return err
	}))
}

func unsavedAppliedCfg(t *testing.T, dataDir string) config.ServerConfig {
	urls, err := types.NewURLs([]string{"http://127.0.0.1:2380"})
	require.NoError(t, err)
	curls, err := types.NewURLs([]string{"http://127.0.0.1:2379"})
	require.NoError(t, err)
	cluster, err := types.NewURLsMap("node0=http://127.0.0.1:2380")
	require.NoError(t, err)
	lg := zaptest.NewLogger(t)
	return config.ServerConfig{
		Name:                   "node0",
		DataDir:                dataDir,
		PeerURLs:               urls,
		ClientURLs:             curls,
		InitialPeerURLsMap:     cluster,
		InitialClusterToken:    "unsaved-applied",
		NewCluster:             true,
		SnapshotCount:          10,
		SnapshotCatchUpEntries: 5,
		TickMs:                 10,
		ElectionTicks:          10,
		MaxTxnOps:              128,
		MaxRequestBytes:        1.5 * 1024 * 1024,
		Logger:                 lg,
		ServerFeatureGate:      features.NewDefaultServerFeatureGate("test", lg),
	}
}

type unsavedAppliedObs struct {
	members       int
	raftVoters    []uint64
	backendCS     []uint64
	snapshotIndex uint64
	snapshotCS    []uint64
}

func sorted(v []uint64) []uint64 {
	v = append([]uint64(nil), v...)
	sort.Slice(v, func(i, j int) bool { return v[i] < v[j] })
	return v
}

// observe waits for the server to lead, writes enough keys for a raft snapshot, and reads the
// membership, raft's configuration, the backend's ConfState and the raft snapshot's ConfState.
func observe(t *testing.T, srv *EtcdServer) unsavedAppliedObs {
	select {
	case <-srv.ReadyNotify():
	case <-time.After(10 * time.Second):
		t.Fatal("server not ready")
	}
	for i := 0; i < 30; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := srv.Put(ctx, &pb.PutRequest{Key: []byte(fmt.Sprintf("k%d", i)), Value: []byte("v")})
		cancel()
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool {
		sn, err := srv.r.raftStorage.Snapshot()
		return err == nil && sn.GetMetadata().GetIndex() > 0
	}, 10*time.Second, 10*time.Millisecond, "no raft snapshot")
	srv.be.ForceCommit()

	var o unsavedAppliedObs
	o.members = len(srv.cluster.Members())
	for id := range srv.r.Status().Config.Voters.IDs() {
		o.raftVoters = append(o.raftVoters, id)
	}
	o.raftVoters = sorted(o.raftVoters)
	tx := srv.be.ReadTx()
	tx.RLock()
	if cs := schema.UnsafeConfStateFromBackend(srv.Logger(), tx); cs != nil {
		o.backendCS = sorted(cs.GetVoters())
	}
	tx.RUnlock()
	sn, err := srv.r.raftStorage.Snapshot()
	require.NoError(t, err)
	o.snapshotIndex = sn.GetMetadata().GetIndex()
	o.snapshotCS = sorted(sn.GetMetadata().GetConfState().GetVoters())
	return o
}

// A member that crashes after its apply thread committed the bootstrap Ready's conf changes to
// the backend and before that Ready's WAL Save reached the disk restarts with an unsaved WAL and
// a backend whose consistent index is the bootstrap's. The torn-Save fix bootstraps it fresh.
// This test compares the restarted member with one that never crashed.
func TestBootstrapUnsavedWALAppliedBackend(t *testing.T) {
	dir := t.TempDir()
	cfg := unsavedAppliedCfg(t, dir)
	srv, err := NewServer(cfg)
	require.NoError(t, err)
	hold := &holdFirstSave{Storage: srv.r.storage, entered: make(chan struct{}), release: make(chan struct{})}
	srv.r.storage = hold
	srv.Start()

	// The raft loop hands the bootstrap Ready's committed entries to the apply thread before it
	// saves the Ready (raftNode.start), so the apply thread applies them while the Save is held.
	<-hold.entered
	require.Eventually(t, func() bool { return srv.getAppliedIndex() >= 1 }, 10*time.Second, time.Millisecond,
		"the bootstrap entries were not applied before the Save")
	// The backend's batch timer commits the apply (BackendBatchInterval); force it here.
	srv.be.ForceCommit()
	ci, _ := schema.ReadConsistentIndex(srv.be.ReadTx())
	require.Equal(t, uint64(1), ci)

	// The crash: the disk holds the WAL without any Save, and the committed backend.
	crashDir := t.TempDir()
	copyTree(t, dir, crashDir)
	unsaved, err := wal.Unsaved(cfg.Logger, filepath.Join(crashDir, "member", "wal"))
	require.NoError(t, err)
	require.True(t, unsaved, "the crash image's WAL holds a Save")

	// The member that did not crash.
	close(hold.release)
	control := observe(t, srv)
	srv.HardStop()

	// The member restarted from the crash image.
	cfg2 := unsavedAppliedCfg(t, crashDir)
	srv2, err := NewServer(cfg2)
	require.NoError(t, err, "restart from an unsaved WAL beside an applied backend")
	srv2.Start()
	restarted := observe(t, srv2)
	srv2.HardStop()

	t.Logf("control:   %+v", control)
	t.Logf("restarted: %+v", restarted)
	assert.Equal(t, control.members, restarted.members, "membership")
	assert.Equal(t, control.raftVoters, restarted.raftVoters, "raft's configuration")
	assert.Equal(t, control.backendCS, restarted.backendCS, "the backend's ConfState")
	assert.Equal(t, control.snapshotCS, restarted.snapshotCS, "the raft snapshot's ConfState")

	// A third start, from the restarted member's WAL.
	cfg3 := unsavedAppliedCfg(t, crashDir)
	srv3, err := NewServer(cfg3)
	require.NoError(t, err, "restart from the WAL")
	srv3.Start()
	again := observe(t, srv3)
	srv3.HardStop()
	t.Logf("again:     %+v", again)
	assert.Equal(t, control.raftVoters, again.raftVoters)
}
