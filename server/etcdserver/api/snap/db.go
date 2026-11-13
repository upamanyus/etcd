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

package snap

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	humanize "github.com/dustin/go-humanize"
	"go.uber.org/zap"

	"go.etcd.io/etcd/client/pkg/v3/fileutil"
)

var ErrNoDBSnapshot = errors.New("snap: snapshot file doesn't exist")

// SaveDBFrom atomically saves a database snapshot from r. Before a successful
// return, it syncs the file and containing directory so the link is durable
// on storage that honors fsync. The receiver processes the Raft message only
// afterward, so its WAL snapshot record cannot precede snap.db. If the
// snapshot of id already exists, SaveDBFrom keeps it and discards r's copy,
// also when another SaveDBFrom of id puts it in place concurrently.
func (s *Snapshotter) SaveDBFrom(r io.Reader, id uint64) (int64, error) {
	start := time.Now()

	f, err := os.CreateTemp(s.dir, "tmp")
	if err != nil {
		return 0, err
	}
	var n int64
	n, err = io.Copy(f, r)
	if err == nil {
		fsyncStart := time.Now()
		err = fileutil.Fsync(f)
		snapDBFsyncSec.Observe(time.Since(fsyncStart).Seconds())
	}
	f.Close()
	if err != nil {
		os.Remove(f.Name())
		return n, err
	}
	fn := s.dbFilePath(id)
	// Link, not rename: a link never replaces an existing file, so of two
	// concurrent saves of one index exactly one puts its file in place and the
	// other keeps it, with no window between a check and a rename.
	err = os.Link(f.Name(), fn)
	os.Remove(f.Name())
	if errors.Is(err, os.ErrExist) {
		// A prior SaveDBFrom can have linked this file and crashed before
		// syncing the directory. Sync again before reporting success.
		if err = s.fsyncDir(s.dir); err != nil {
			return n, err
		}
		return n, nil
	}
	if err != nil {
		return n, err
	}

	// gofail: var snapDBRenameBeforeDirSync struct{}

	// The file is synced above. Sync the directory to make the linked entry
	// durable before the snapshot receiver processes the Raft message.
	if err = s.fsyncDir(s.dir); err != nil {
		s.lg.Warn(
			"failed to fsync snap directory after saving database snapshot",
			zap.String("path", fn),
			zap.Error(err),
		)
		return n, err
	}

	s.lg.Info(
		"saved database snapshot to disk",
		zap.String("path", fn),
		zap.Int64("bytes", n),
		zap.String("size", humanize.Bytes(uint64(n))),
	)

	snapDBSaveSec.Observe(time.Since(start).Seconds())
	return n, nil
}

// DBFilePath returns the file path for the snapshot of the database with
// given id. If the snapshot does not exist, it returns error.
func (s *Snapshotter) DBFilePath(id uint64) (string, error) {
	if _, err := fileutil.ReadDir(s.dir); err != nil {
		return "", err
	}
	fn := s.dbFilePath(id)
	if fileutil.Exist(fn) {
		return fn, nil
	}
	if s.lg != nil {
		s.lg.Warn(
			"failed to find [SNAPSHOT-INDEX].snap.db",
			zap.Uint64("snapshot-index", id),
			zap.String("snapshot-file-path", fn),
			zap.Error(ErrNoDBSnapshot),
		)
	}
	return "", ErrNoDBSnapshot
}

func (s *Snapshotter) dbFilePath(id uint64) string {
	return filepath.Join(s.dir, fmt.Sprintf("%016x.snap.db", id))
}

// fsyncSnapDir makes a renamed snapshot database durable. On Linux, syncing
// the file alone does not necessarily persist its containing directory entry.
func fsyncSnapDir(dir string) error {
	// gofail: var snapDBDirSyncError string
	// return errors.New(snapDBDirSyncError)
	d, err := fileutil.OpenDir(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return fileutil.Fsync(d)
}
