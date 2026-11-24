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

package backend

import (
	"path/filepath"
	"testing"
)

// TestSyncParentDir checks that defrag's directory sync opens and fsyncs the
// directory holding the database, and reports a missing directory.
func TestSyncParentDir(t *testing.T) {
	dir := t.TempDir()
	if err := syncParentDir(filepath.Join(dir, "db")); err != nil {
		t.Fatalf("syncParentDir of an existing directory: %v", err)
	}
	if err := syncParentDir(filepath.Join(dir, "missing", "db")); err == nil {
		t.Fatal("syncParentDir of a missing directory: want an error")
	}
}
