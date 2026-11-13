// Copyright 2018 The etcd Authors
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
	"bytes"
	"math"
	"os"
	"path/filepath"
	"testing"

	"go.uber.org/zap/zaptest"
)

func TestFilePipeline(t *testing.T) {
	tdir := t.TempDir()

	fp := newFilePipeline(zaptest.NewLogger(t), tdir, SegmentSizeBytes)
	defer fp.Close()

	f, ferr := fp.Open()
	if ferr != nil {
		t.Fatal(ferr)
	}
	f.Close()
}

func TestFilePipelineFailPreallocate(t *testing.T) {
	tdir := t.TempDir()

	fp := newFilePipeline(zaptest.NewLogger(t), tdir, math.MaxInt64)
	defer fp.Close()

	f, ferr := fp.Open()
	if f != nil || ferr == nil { // no space left on device
		t.Fatal("expected error on invalid pre-allocate size, but no error")
	}
}

// TestFilePipelineTruncatesStaleTmp: a 0.tmp left by an interrupted cut is
// reused empty: every byte of the preallocated file is zero.
func TestFilePipelineTruncatesStaleTmp(t *testing.T) {
	tdir := t.TempDir()
	stale := bytes.Repeat([]byte{0xab}, 4096)
	if err := os.WriteFile(filepath.Join(tdir, "0.tmp"), stale, 0o600); err != nil {
		t.Fatal(err)
	}

	size := int64(64 * 1024)
	fp := newFilePipeline(zaptest.NewLogger(t), tdir, size)
	defer fp.Close()

	f, ferr := fp.Open()
	if ferr != nil {
		t.Fatal(ferr)
	}
	name := f.Name()
	f.Close()
	b, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(b)) != size {
		t.Fatalf("size %d, want %d", len(b), size)
	}
	for i, c := range b {
		if c != 0 {
			t.Fatalf("stale byte %#x at offset %d of the reused %s", c, i, filepath.Base(name))
		}
	}
}
