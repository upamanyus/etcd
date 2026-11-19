package wal

import (
	"errors"
	"testing"

	"go.uber.org/zap/zaptest"

	"go.etcd.io/raft/v3/raftpb"
)

// TestSaveBatchTooLarge: a batch above MaxSaveBatchBytes is refused before anything is
// written, and the WAL keeps working: a later Save succeeds and ReadAll sees only it.
func TestSaveBatchTooLarge(t *testing.T) {
	old := MaxSaveBatchBytes
	defer func() { MaxSaveBatchBytes = old }()
	MaxSaveBatchBytes = 4096

	dir := t.TempDir()
	w, err := Create(zaptest.NewLogger(t), dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	big := make([]*raftpb.Entry, 0, 8)
	for i := uint64(1); i <= 8; i++ {
		big = append(big, &raftpb.Entry{Index: new(i), Term: new(uint64(1)), Data: make([]byte, 1024)})
	}
	if err := w.Save(&raftpb.HardState{Term: new(uint64(1)), Commit: new(uint64(0))}, big); !errors.Is(err, ErrSaveBatchTooLarge) {
		t.Fatalf("Save of a large batch: err = %v, want ErrSaveBatchTooLarge", err)
	}
	if err := w.Save(&raftpb.HardState{Term: new(uint64(1)), Commit: new(uint64(1))}, big[:1]); err != nil {
		t.Fatalf("Save after the refused batch: %v", err)
	}
	w.Close()

	w, err = Open(zaptest.NewLogger(t), dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	_, st, ents, err := w.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(ents) != 1 || st.GetCommit() != 1 {
		t.Fatalf("ReadAll: %d entries, commit %d; want 1 entry, commit 1", len(ents), st.GetCommit())
	}
}
