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

package rafthttp

import (
	"testing"

	"go.uber.org/zap"

	"go.etcd.io/etcd/client/pkg/v3/types"
	stats "go.etcd.io/etcd/server/v3/etcdserver/api/v2stats"
)

// A remote may be cut, mended and stopped any number of times, in any order.
func TestTransportCutPeerTwiceRemote(t *testing.T) {
	tr := &Transport{
		Logger:      zap.NewNop(),
		ID:          types.ID(1),
		ServerStats: stats.NewServerStats("", ""),
		LeaderStats: stats.NewLeaderStats(zap.NewNop(), ""),
		peers:       map[types.ID]Peer{},
		remotes:     map[types.ID]*remote{},
	}
	tr.AddRemote(types.ID(2), []string{"http://127.0.0.1:0"})
	tr.CutPeer(types.ID(2))
	tr.CutPeer(types.ID(2))
	tr.MendPeer(types.ID(2))
	tr.MendPeer(types.ID(2))
	tr.CutPeer(types.ID(2))
	tr.remotes[types.ID(2)].stop() // what Transport.Stop does for each remote
}
