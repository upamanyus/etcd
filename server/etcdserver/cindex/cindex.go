// Copyright 2015 The etcd Authors
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

package cindex

import (
	"sync"
	"sync/atomic"

	"go.etcd.io/etcd/server/v3/storage/backend"
	"go.etcd.io/etcd/server/v3/storage/schema"
)

type Backend interface {
	ReadTx() backend.ReadTx
}

// ConsistentIndexer is an interface that wraps the Get/Set/Save method for consistentIndex.
type ConsistentIndexer interface {
	// ConsistentIndex returns the consistent index of current executing entry.
	ConsistentIndex() uint64

	// ConsistentApplyingIndex returns the consistent applying index of current executing entry.
	ConsistentApplyingIndex() (uint64, uint64)

	// UnsafeConsistentIndex is similar to ConsistentIndex, but it doesn't lock the transaction.
	UnsafeConsistentIndex() uint64

	// SetConsistentIndex set the consistent index of current executing entry.
	SetConsistentIndex(v uint64, term uint64)

	// SetConsistentApplyingIndex set the consistent applying index of current executing entry.
	SetConsistentApplyingIndex(v uint64, term uint64)

	// SetRaftTerm sets the member's raft term that UnsafeSave saves with the consistent index:
	// it must be at least the term in which the member learned that the entries applied from
	// now on are committed. It never decreases.
	SetRaftTerm(raftTerm uint64)

	// UnsafeSave must be called holding the lock on the tx.
	// It saves consistentIndex to the underlying stable storage.
	UnsafeSave(tx backend.UnsafeReadWriter)

	// SetBackend set the available backend.BatchTx for ConsistentIndexer.
	SetBackend(be Backend)
}

// consistentIndex implements the ConsistentIndexer interface.
type consistentIndex struct {
	// consistentIndex represents the offset of an entry in a consistent replica log.
	// It caches the "consistent_index" key's value.
	// Accessed through atomics so must be 64-bit aligned.
	consistentIndex uint64
	// term represents the RAFT term of committed entry in a consistent replica log.
	// Accessed through atomics so must be 64-bit aligned.
	// The value is being persisted in the backend since v3.5.
	term uint64

	// applyingIndex and applyingTerm are just temporary cache of the raftpb.Entry.Index
	// and raftpb.Entry.Term, and they are not ready to be persisted yet. They will be
	// saved to consistentIndex and term above in the txPostLockInsideApplyHook.
	//
	// TODO(ahrtr): try to remove the OnPreCommitUnsafe, and compare the
	//  performance difference. Afterwards we can make a decision on whether
	//  or not we should remove OnPreCommitUnsafe. If it is true, then we
	//  can remove applyingIndex and applyingTerm, and save the e.Index and
	//  e.Term to consistentIndex and term directly in applyEntries, and
	//  persist them into db in the txPostLockInsideApplyHook.
	applyingIndex uint64
	applyingTerm  uint64

	// be is used for initial read consistentIndex
	be Backend
	// mutex is protecting be.
	mutex sync.Mutex
	// raftTerm is the member's raft term, at least the term in which it learned that the
	// entries applied since it was set are committed (SetRaftTerm). UnsafeSave saves it with
	// consistentIndex. Guarded by pairMu.
	raftTerm uint64

	// pairMu makes consistentIndex, term and raftTerm one triple for UnsafeSave: the apply loop
	// sets them outside the backend's batch lock (applyEntryNormal, a refused conf change), while
	// the backend's commit saves them, and the saved term must be the term of the saved index, the
	// saved raft term a term in which the member knew the saved index committed.
	pairMu sync.Mutex
}

// NewConsistentIndex creates a new consistent index.
// If `be` is nil, it must be set (SetBackend) before first access using `ConsistentIndex()`.
func NewConsistentIndex(be Backend) ConsistentIndexer {
	return &consistentIndex{be: be}
}

func (ci *consistentIndex) ConsistentIndex() uint64 {
	if index := atomic.LoadUint64(&ci.consistentIndex); index > 0 {
		return index
	}
	ci.mutex.Lock()
	defer ci.mutex.Unlock()

	v, term := schema.ReadConsistentIndex(ci.be.ReadTx())
	ci.SetConsistentIndex(v, term)
	return v
}

func (ci *consistentIndex) UnsafeConsistentIndex() uint64 {
	if index := atomic.LoadUint64(&ci.consistentIndex); index > 0 {
		return index
	}

	v, term := schema.UnsafeReadConsistentIndex(ci.be.ReadTx())
	ci.SetConsistentIndex(v, term)
	return v
}

func (ci *consistentIndex) SetConsistentIndex(v uint64, term uint64) {
	ci.pairMu.Lock()
	defer ci.pairMu.Unlock()
	atomic.StoreUint64(&ci.consistentIndex, v)
	atomic.StoreUint64(&ci.term, term)
}

func (ci *consistentIndex) SetRaftTerm(raftTerm uint64) {
	ci.pairMu.Lock()
	defer ci.pairMu.Unlock()
	if raftTerm > ci.raftTerm {
		ci.raftTerm = raftTerm
	}
}

func (ci *consistentIndex) UnsafeSave(tx backend.UnsafeReadWriter) {
	ci.pairMu.Lock()
	index := atomic.LoadUint64(&ci.consistentIndex)
	term := atomic.LoadUint64(&ci.term)
	raftTerm := ci.raftTerm
	ci.pairMu.Unlock()
	schema.UnsafeUpdateConsistentIndex(tx, index, term)
	schema.UnsafeUpdateConsistentIndexRaftTerm(tx, index, raftTerm)
}

func (ci *consistentIndex) SetBackend(be Backend) {
	ci.mutex.Lock()
	defer ci.mutex.Unlock()
	ci.be = be
	// After the backend is changed, the first access should re-read it.
	ci.SetConsistentIndex(0, 0)
}

func (ci *consistentIndex) ConsistentApplyingIndex() (uint64, uint64) {
	return atomic.LoadUint64(&ci.applyingIndex), atomic.LoadUint64(&ci.applyingTerm)
}

func (ci *consistentIndex) SetConsistentApplyingIndex(v uint64, term uint64) {
	atomic.StoreUint64(&ci.applyingIndex, v)
	atomic.StoreUint64(&ci.applyingTerm, term)
}

func NewFakeConsistentIndex(index uint64) ConsistentIndexer {
	return &fakeConsistentIndex{index: index}
}

type fakeConsistentIndex struct {
	index uint64
	term  uint64
}

func (f *fakeConsistentIndex) ConsistentIndex() uint64 {
	return atomic.LoadUint64(&f.index)
}

func (f *fakeConsistentIndex) ConsistentApplyingIndex() (uint64, uint64) {
	return atomic.LoadUint64(&f.index), atomic.LoadUint64(&f.term)
}

func (f *fakeConsistentIndex) UnsafeConsistentIndex() uint64 {
	return atomic.LoadUint64(&f.index)
}

func (f *fakeConsistentIndex) SetConsistentIndex(index uint64, term uint64) {
	atomic.StoreUint64(&f.index, index)
	atomic.StoreUint64(&f.term, term)
}

func (f *fakeConsistentIndex) SetConsistentApplyingIndex(index uint64, term uint64) {
	atomic.StoreUint64(&f.index, index)
	atomic.StoreUint64(&f.term, term)
}

func (f *fakeConsistentIndex) SetRaftTerm(_ uint64)                  {}
func (f *fakeConsistentIndex) UnsafeSave(_ backend.UnsafeReadWriter) {}
func (f *fakeConsistentIndex) SetBackend(_ Backend)                  {}

func UpdateConsistentIndexForce(tx backend.BatchTx, index uint64, term uint64) {
	tx.LockOutsideApply()
	defer tx.Unlock()
	schema.UnsafeUpdateConsistentIndexForce(tx, index, term)
}
