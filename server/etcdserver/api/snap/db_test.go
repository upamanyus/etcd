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

package snap

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestSaveDBFromSyncsDirectory(t *testing.T) {
	for _, tc := range []struct {
		name     string
		existing bool
	}{
		{name: "new"},
		{name: "existing", existing: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			s := New(nil, dir)
			if tc.existing {
				writeErr := os.WriteFile(s.dbFilePath(1), []byte("old"), 0o600)
				if writeErr != nil {
					t.Fatal(writeErr)
				}
			}

			syncErr := errors.New("sync failed")
			syncCalls := 0
			s.fsyncDir = func(gotDir string) error {
				if gotDir != dir {
					t.Errorf("fsync directory = %q, want %q", gotDir, dir)
				}
				syncCalls++
				return syncErr
			}

			const content = "snapshot"
			n, err := s.SaveDBFrom(strings.NewReader(content), 1)
			if !errors.Is(err, syncErr) {
				t.Errorf("SaveDBFrom error = %v, want %v", err, syncErr)
			}
			if n != int64(len(content)) {
				t.Errorf("SaveDBFrom bytes = %d, want %d", n, len(content))
			}
			if syncCalls != 1 {
				t.Errorf("fsync calls = %d, want 1", syncCalls)
			}
		})
	}
}

// gatedReader returns data once gate is closed, then EOF.
type gatedReader struct {
	data []byte
	gate chan struct{}
	done bool
}

func (g *gatedReader) Read(p []byte) (int, error) {
	if g.done {
		return 0, io.EOF
	}
	<-g.gate
	g.done = true
	return copy(p, g.data), nil
}

// TestSaveDBFromConcurrentSameIndex runs two saves of one index with different
// data at once, from two snapshotters of one directory (as two incarnations of
// a member's snapshot handler can). The first save to put its file in place
// wins: after both return the file holds that save's data, the other save kept
// it, and no temporary file is left.
func TestSaveDBFromConcurrentSameIndex(t *testing.T) {
	const iterations = 500
	for it := 0; it < iterations; it++ {
		dir := t.TempDir()
		gate := make(chan struct{})
		datas := [][]byte{bytes.Repeat([]byte{'A'}, 64), bytes.Repeat([]byte{'B'}, 64)}
		logs := make([]*observer.ObservedLogs, len(datas))
		var wg sync.WaitGroup
		for k := range datas {
			core, l := observer.New(zap.InfoLevel)
			logs[k] = l
			s := New(zap.New(core), dir)
			r := &gatedReader{data: datas[k], gate: gate}
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, err := s.SaveDBFrom(r, 5); err != nil {
					t.Errorf("SaveDBFrom: %v", err)
				}
			}()
		}
		close(gate)
		wg.Wait()

		winner := -1
		for k := range datas {
			if logs[k].FilterMessage("saved database snapshot to disk").Len() > 0 {
				if winner >= 0 {
					t.Fatalf("iteration %d: both saves put their file in place", it)
				}
				winner = k
			}
		}
		if winner < 0 {
			t.Fatalf("iteration %d: no save put its file in place", it)
		}
		got, err := os.ReadFile(filepath.Join(dir, fmt.Sprintf("%016x.snap.db", 5)))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, datas[winner]) {
			t.Fatalf("iteration %d: file holds %q, want the first writer's %q", it, got, datas[winner])
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 1 {
			t.Fatalf("iteration %d: %d entries in the directory, want 1", it, len(entries))
		}
	}
}
