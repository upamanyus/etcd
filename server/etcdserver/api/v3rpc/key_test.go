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

package v3rpc

import (
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
)

func TestCheckRangeRequest(t *testing.T) {
	rangeReqs := []struct {
		sortOrder     pb.RangeRequest_SortOrder
		sortTarget    pb.RangeRequest_SortTarget
		expectedError error
	}{
		{
			sortOrder:     pb.RangeRequest_ASCEND,
			sortTarget:    pb.RangeRequest_CREATE,
			expectedError: nil,
		},
		{
			sortOrder:     pb.RangeRequest_ASCEND,
			sortTarget:    100,
			expectedError: rpctypes.ErrGRPCInvalidSortOption,
		},
		{
			sortOrder:     200,
			sortTarget:    pb.RangeRequest_MOD,
			expectedError: rpctypes.ErrGRPCInvalidSortOption,
		},
	}

	for _, req := range rangeReqs {
		rangeReq := pb.RangeRequest{
			Key:        []byte{1, 2, 3},
			SortOrder:  req.sortOrder,
			SortTarget: req.sortTarget,
		}

		actualRet := checkRangeRequest(&rangeReq)
		if getError(actualRet) != getError(req.expectedError) {
			t.Errorf("expected sortOrder (%d) and sortTarget (%d) to be %q, but got %q",
				req.sortOrder, req.sortTarget, getError(req.expectedError), getError(actualRet))
		}
	}
}

func getError(err error) string {
	if err == nil {
		return ""
	}

	return err.Error()
}

func TestCheckIntervalsFromKeyDelete(t *testing.T) {
	// a DeleteRange with RangeEnd "\x00" deletes every key from Key on
	// (clientv3.WithFromKey)
	delFromKey := func(key string) *pb.RequestOp {
		return &pb.RequestOp{Request: &pb.RequestOp_RequestDeleteRange{
			RequestDeleteRange: &pb.DeleteRangeRequest{Key: []byte(key), RangeEnd: []byte{0}},
		}}
	}
	put := func(key string) *pb.RequestOp {
		return &pb.RequestOp{Request: &pb.RequestOp_RequestPut{
			RequestPut: &pb.PutRequest{Key: []byte(key), IgnoreValue: true},
		}}
	}
	nested := func(ops ...*pb.RequestOp) *pb.RequestOp {
		return &pb.RequestOp{Request: &pb.RequestOp_RequestTxn{
			RequestTxn: &pb.TxnRequest{Success: ops},
		}}
	}

	tests := []struct {
		name string
		reqs []*pb.RequestOp
		werr error
	}{
		{"put of the key", []*pb.RequestOp{delFromKey("a"), put("a")}, rpctypes.ErrGRPCDuplicateKey},
		{"put of a greater key", []*pb.RequestOp{delFromKey("a"), put("zzz")}, rpctypes.ErrGRPCDuplicateKey},
		{"put before the delete", []*pb.RequestOp{put("a"), delFromKey("a")}, rpctypes.ErrGRPCDuplicateKey},
		{"put in a nested txn", []*pb.RequestOp{delFromKey("a"), nested(put("b"))}, rpctypes.ErrGRPCDuplicateKey},
		{"delete in a nested txn", []*pb.RequestOp{nested(delFromKey("a")), put("b")}, rpctypes.ErrGRPCDuplicateKey},
		{"delete of every key", []*pb.RequestOp{delFromKey("\x00"), put("a")}, rpctypes.ErrGRPCDuplicateKey},
		{"put of a smaller key", []*pb.RequestOp{delFromKey("b"), put("a")}, nil},
		{"put of a smaller key in a nested txn", []*pb.RequestOp{delFromKey("b"), nested(put("a"))}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := checkIntervals(tt.reqs)
			if getError(err) != getError(tt.werr) {
				t.Errorf("checkIntervals: got %v, want %v", err, tt.werr)
			}
		})
	}
}
