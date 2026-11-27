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

// Scratch-only hooks for the keep-guard repro. They only pause goroutines
// (a legal schedule); they never change what the server computes.

import (
	"sync/atomic"

	"go.etcd.io/raft/v3/raftpb"
)

type ReproHooks struct {
	// Called in the raft loop just before r.storage.Save. May block.
	BeforeSave func(r any, ents []*raftpb.Entry, committed []*raftpb.Entry)
	// Called in applyAll at "start", "beforeSnapshot", "beforeMsgSnap". May block.
	ApplyAll func(s *EtcdServer, stage string, appliedi uint64)
	// Called at the end of applySnapshot. May block.
	AfterApplySnapshot func(s *EtcdServer, index uint64)
}

var reproHooks atomic.Pointer[ReproHooks]

func SetReproHooks(h *ReproHooks) { reproHooks.Store(h) }

func (s *EtcdServer) ReproRaftNode() any         { return &s.r }
func (s *EtcdServer) ReproConsistentIndex() uint64 { return s.consistIndex.ConsistentIndex() }
func (s *EtcdServer) ReproPendingMsgSnap() int   { return len(s.r.msgSnapC) }
func (s *EtcdServer) ReproAppliedIndex() uint64  { return s.getAppliedIndex() }

func reproApplyAll(s *EtcdServer, stage string, appliedi uint64) {
	if h := reproHooks.Load(); h != nil && h.ApplyAll != nil {
		h.ApplyAll(s, stage, appliedi)
	}
}

func (s *EtcdServer) ReproRaftCommit() uint64 { return s.r.Status().GetCommit() }
func (s *EtcdServer) ReproRaftVoters() []uint64 {
	var vs []uint64
	for id := range s.r.Status().Config.Voters.IDs() {
		vs = append(vs, id)
	}
	return vs
}
