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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"google.golang.org/protobuf/proto"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/server/v3/config"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/raft/v3/raftpb"
)

// A member that crashes after its first bootstrap created the WAL (and the backend) and before
// the first Save bootstraps again on restart, as a member with no WAL: raft starts with its
// peers (StartNode), not from the empty WAL (RestartNode with no configuration). Once a Save
// has written state, the restart takes the WAL.
func TestBootstrapUnsavedWALBootstrapsFresh(t *testing.T) {
	urls, err := types.NewURLs([]string{"http://127.0.0.1:2380"})
	require.NoError(t, err)
	cluster, err := types.NewURLsMap("node0=http://127.0.0.1:2380")
	require.NoError(t, err)
	cfg := config.ServerConfig{
		Name:                "node0",
		DataDir:             t.TempDir(),
		PeerURLs:            urls,
		InitialPeerURLsMap:  cluster,
		InitialClusterToken: "tornsave",
		NewCluster:          true,
		BackendFreelistType: bolt.FreelistArrayType,
		TickMs:              100,
		ElectionTicks:       10,
		Logger:              zaptest.NewLogger(t),
	}

	boot := func() *bootstrappedServer {
		b, err := bootstrap(cfg)
		require.NoError(t, err)
		return b
	}
	closeAll := func(b *bootstrappedServer) {
		require.NoError(t, b.storage.wal.w.Close())
		b.Close()
	}

	// First boot: no WAL. It creates the WAL and the backend; then a crash, before any Save.
	b := boot()
	assert.False(t, b.storage.wal.haveWAL)
	assert.Len(t, b.raft.peers, 1)
	closeAll(b)
	require.True(t, wal.Exist(cfg.WALDir()))

	// The restart finds the WAL and a backend, and bootstraps again.
	b = boot()
	assert.False(t, b.storage.wal.haveWAL, "the unsaved WAL was taken as a saved one")
	assert.Len(t, b.raft.peers, 1, "raft restarted from an empty log, not bootstrapped")

	// The first Save (raft's bootstrap Ready: the conf change entry and its commit), then a
	// crash: the restart takes the WAL.
	id := uint64(b.cluster.nodeID)
	cc := &raftpb.ConfChange{Type: raftpb.ConfChangeAddNode.Enum(), NodeId: new(id), Context: b.raft.peers[0].Context}
	data, err := proto.Marshal(cc)
	require.NoError(t, err)
	ent := &raftpb.Entry{Type: raftpb.EntryConfChange.Enum(), Term: new(uint64(1)), Index: new(uint64(1)), Data: data}
	require.NoError(t, b.storage.wal.w.Save(&raftpb.HardState{Term: new(uint64(1)), Commit: new(uint64(1))}, []*raftpb.Entry{ent}))
	closeAll(b)

	b = boot()
	assert.True(t, b.storage.wal.haveWAL)
	assert.Empty(t, b.raft.peers)
	assert.Equal(t, uint64(1), b.storage.wal.st.GetCommit())
	assert.Len(t, b.storage.wal.ents, 1)
	closeAll(b)
}
