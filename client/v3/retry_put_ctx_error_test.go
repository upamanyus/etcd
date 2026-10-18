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

package clientv3

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

// putCountingKV applies every Put it receives (counts it), and answers the first one with a
// gRPC Canceled status, as a server whose handler context was canceled after the write was
// applied would (etcd's server maps a canceled proposal to rpctypes.ErrGRPCCanceled).
type putCountingKV struct {
	pb.UnimplementedKVServer
	puts atomic.Int32
}

func (s *putCountingKV) Put(ctx context.Context, r *pb.PutRequest) (*pb.PutResponse, error) {
	if s.puts.Add(1) == 1 {
		return nil, status.Error(codes.Canceled, "etcdserver: request canceled")
	}
	return &pb.PutResponse{Header: &pb.ResponseHeader{Revision: 2}}, nil
}

// A Put whose attempt fails with a Canceled status while the caller's context is live is
// not resent (write-at-most-once).
func TestPutNotRetriedAfterCanceledStatus(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	srv := grpc.NewServer()
	kvs := &putCountingKV{}
	pb.RegisterKVServer(srv, kvs)
	go srv.Serve(lis)
	defer srv.Stop()

	c, err := New(Config{Endpoints: []string{lis.Addr().String()}, DialTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// The first attempt may have been applied: the client must return its error, not resend.
	if _, err := c.Put(ctx, "k", "v"); status.Code(err) != codes.Canceled {
		t.Fatalf("Put: got %v, want the attempt's Canceled status", err)
	}
	if n := kvs.puts.Load(); n != 1 {
		t.Errorf("the server received the Put %d times: the client retried a non-repeatable request", n)
	}
}
