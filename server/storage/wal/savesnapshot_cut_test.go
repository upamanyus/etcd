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

package wal

import (
	"io"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3/raftpb"
)

// TestSaveSnapshotCuts checks that snapshot records alone, saved with no
// entry-writing Save between them, cut the tail once it passes
// SegmentSizeBytes, as Save does, and that the WAL then opens at the last
// snapshot with its hard state.
func TestSaveSnapshotCuts(t *testing.T) {
	p := t.TempDir()

	restoreLater := SegmentSizeBytes
	SegmentSizeBytes = 2 * 1024
	defer func() { SegmentSizeBytes = restoreLater }()

	w, err := Create(zaptest.NewLogger(t), p, []byte("metadata"))
	require.NoError(t, err)

	state := &raftpb.HardState{Term: new(uint64(1)), Commit: new(uint64(0))}
	require.NoError(t, w.Save(state, nil))

	// A ConfState with many voters makes each record a few hundred bytes.
	cs := &raftpb.ConfState{}
	for v := uint64(1); v <= 40; v++ {
		cs.Voters = append(cs.Voters, v)
	}
	var last *walpb.Snapshot
	for i := uint64(1); i <= 64; i++ {
		last = &walpb.Snapshot{Index: new(i), Term: new(uint64(1)), ConfState: cs}
		require.NoError(t, w.SaveSnapshot(last))
	}
	names, err := readWALNames(zaptest.NewLogger(t), p)
	require.NoError(t, err)
	require.Greaterf(t, len(names), 1, "SaveSnapshot never cut: %v", names)
	off, err := w.tail().Seek(0, io.SeekCurrent)
	require.NoError(t, err)
	require.Less(t, off, SegmentSizeBytes)
	require.NoError(t, w.Close())

	nw, err := Open(zaptest.NewLogger(t), p, &walpb.Snapshot{Index: last.Index, Term: last.Term})
	require.NoError(t, err)
	defer nw.Close()
	_, idx, err := parseWALName(filepath.Base(nw.tail().Name()))
	require.NoError(t, err)
	require.LessOrEqual(t, idx, last.GetIndex()+1)
	_, st, ents, err := nw.ReadAll()
	require.NoError(t, err)
	require.Equal(t, state.GetTerm(), st.GetTerm())
	require.Empty(t, ents)
}
