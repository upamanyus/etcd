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

package backend_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/etcd/server/v3/storage/backend"
	"go.etcd.io/etcd/server/v3/storage/schema"
)

// A power loss during the first start's bolt.Open, between the two syncs of bbolt's init, can
// leave both meta pages torn over the synced freelist and root pages. The backend opens such a
// file (bbolt initializes it again) instead of panicking ("failed to open database").
func TestBackendOpensInterruptedInit(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	db, err := bolt.Open(path, 0o600, nil)
	require.NoError(t, err)
	ps := db.Info().PageSize
	require.NoError(t, db.Close())
	image, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Len(t, image, 4*ps)

	// Every other byte of each meta page lost; the freelist and root pages whole.
	torn := append([]byte(nil), image...)
	for i := 0; i < 2*ps; i += 2 {
		torn[i] = 0
	}
	require.NoError(t, os.WriteFile(path, torn, 0o600))

	be := backend.NewDefaultBackend(zaptest.NewLogger(t), path)
	tx := be.BatchTx()
	tx.Lock()
	tx.UnsafeCreateBucket(schema.Test)
	tx.UnsafePut(schema.Test, []byte("k"), []byte("v"))
	tx.Unlock()
	be.ForceCommit()
	require.NoError(t, be.Close())
}
