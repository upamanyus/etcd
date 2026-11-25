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
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"go.uber.org/zap"

	"go.etcd.io/etcd/pkg/v3/pbutil"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
)

// Unsaved reports whether the WAL in dirpath holds nothing a Save or a SaveSnapshot wrote: read
// from its first segment (sequence 0, index 0), it has no state record, no snapshot record but
// Create's (index 0), and no entry record that another record follows (ReadAll drops the entry
// records no record follows: those of a Save a crash interrupted). It is what Create leaves,
// possibly with part of a first Save that a crash interrupted: the raft state it holds is the
// initial one. An error reading the WAL, other than a torn last record, is returned with false.
func Unsaved(lg *zap.Logger, dirpath string) (bool, error) {
	if lg == nil {
		lg = zap.NewNop()
	}
	names, err := readWALNames(lg, dirpath)
	if err != nil {
		return false, err
	}
	seq, index, err := parseWALName(names[0])
	if err != nil {
		return false, err
	}
	if seq != 0 || index != 0 || !isValidSeq(lg, names) {
		return false, nil
	}
	rs, _, closer, err := openWALFiles(lg, dirpath, names, 0, false)
	if err != nil {
		return false, err
	}
	defer closer()

	decoder := NewDecoder(rs...)
	rec := &walpb.Record{}
	entries := false
	for err = decoder.Decode(rec); err == nil; err = decoder.Decode(rec) {
		if entries && rec.GetType() != EntryType {
			// a record follows entry records: they count
			return false, nil
		}
		switch rec.GetType() {
		case EntryType:
			entries = true
		case StateType:
			return false, nil
		case SnapshotType:
			var snap walpb.Snapshot
			pbutil.MustUnmarshalMessage(&snap, rec.Data)
			if snap.GetIndex() != 0 {
				return false, nil
			}
		case MetadataType:
		case CrcType:
			crc := decoder.LastCRC()
			if crc != 0 && rec.Validate(crc) != nil {
				return false, ErrCRCMismatch
			}
			decoder.UpdateCRC(rec.GetCrc())
		default:
			return false, fmt.Errorf("unexpected block type %d", rec.GetType())
		}
	}
	if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return false, err
	}
	return true, nil
}

// Discard removes the WAL in dirpath, which the caller found unsaved (Unsaved returned true,
// nil) and whose backend it removed. The WAL directory is renamed to Create's temporary
// directory (which Create removes) and the rename made durable, so that Exist reports no WAL,
// atomically across a crash. Discard does not check Unsaved again: a second check could answer
// differently from the caller's (a transient read error in one of them), and the caller's check
// is what its removal of the backend relied on.
func Discard(lg *zap.Logger, dirpath string) error {
	if lg == nil {
		lg = zap.NewNop()
	}
	dir := filepath.Clean(dirpath)
	tmpdirpath := dir + ".tmp"
	if err := os.RemoveAll(tmpdirpath); err != nil {
		return err
	}
	if err := os.Rename(dir, tmpdirpath); err != nil {
		return err
	}
	if err := syncDir(filepath.Dir(dir)); err != nil {
		return err
	}
	lg.Info("discarded a WAL that holds no saved state", zap.String("wal-dir", dirpath))
	return nil
}
