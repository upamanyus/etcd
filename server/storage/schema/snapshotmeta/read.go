// Copyright 2021 The etcd Authors
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

// Package snapshotmeta reads membership metadata from an existing backend transaction.
package snapshotmeta

import (
	"encoding/json"
	"fmt"
	"log"

	"go.uber.org/zap"

	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/server/v3/etcdserver/api/membership"
	"go.etcd.io/etcd/server/v3/storage/backend"
	"go.etcd.io/raft/v3/raftpb"
)

// ReadConfState returns nil when the database predates persisted ConfState.
func ReadConfState(lg *zap.Logger, tx backend.UnsafeReader, meta backend.Bucket, key []byte) *raftpb.ConfState {
	keys, vals := tx.UnsafeRange(meta, key, nil, 0)
	if len(keys) == 0 {
		return nil
	}

	if len(keys) != 1 {
		lg.Panic(
			"unexpected number of key: "+string(key)+" when getting cluster version from backend",
			zap.Int("number-of-key", len(keys)),
		)
	}
	var confState raftpb.ConfState
	if err := json.Unmarshal(vals[0], &confState); err != nil {
		log.Panic("Cannot unmarshal confState json retrieved from the backend",
			zap.ByteString("conf-state-json", vals[0]),
			zap.Error(err))
	}
	return &confState
}

// ReadMembers decodes the membership in the supplied bucket at this transaction.
func ReadMembers(lg *zap.Logger, tx backend.UnsafeReader, bucket backend.Bucket) (map[types.ID]*membership.Member, error) {
	members := make(map[types.ID]*membership.Member)
	err := tx.UnsafeForEach(bucket, func(k, v []byte) error {
		memberID := MustParseMemberID(lg, k)
		m := &membership.Member{ID: memberID}
		if err := json.Unmarshal(v, &m); err != nil {
			return err
		}
		members[memberID] = m
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("couldn't read members from backend: %w", err)
	}

	return members, nil
}

// MustParseMemberID parses a backend membership key.
func MustParseMemberID(lg *zap.Logger, key []byte) types.ID {
	id, err := types.IDFromString(string(key))
	if err != nil {
		lg.Panic("failed to parse member id from key", zap.Error(err))
	}
	return id
}
