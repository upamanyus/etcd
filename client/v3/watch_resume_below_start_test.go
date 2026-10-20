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
	"testing"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

// A fake gRPC watch stream; the test plays the server.
type belowStartStream struct {
	grpc.ClientStream
	ctx  context.Context
	sent chan *pb.WatchRequest
	recv chan belowStartRecv
}

type belowStartRecv struct {
	resp *pb.WatchResponse
	err  error
}

func (s *belowStartStream) Send(r *pb.WatchRequest) error { s.sent <- r; return nil }

func (s *belowStartStream) Recv() (*pb.WatchResponse, error) {
	select {
	case r := <-s.recv:
		return r.resp, r.err
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}

type belowStartClient struct{ streams chan *belowStartStream }

func (c *belowStartClient) Watch(ctx context.Context, _ ...grpc.CallOption) (pb.Watch_WatchClient, error) {
	s := &belowStartStream{ctx: ctx, sent: make(chan *pb.WatchRequest, 16), recv: make(chan belowStartRecv, 16)}
	c.streams <- s
	return s, nil
}

// TestWatchResumeNotBelowStartRevision: a watch from a revision the server has not reached yet
// (10, the store at 5) gets a progress notification at the store's revision (etcd sends one to
// a synced watcher: WithProgressNotify, or RequestProgress's broadcast); the stream then fails
// transiently. The resumed create must not start below the watch's start revision: from 6 the
// watch would deliver the events of revisions 6 to 9, which the caller did not ask for.
func TestWatchResumeNotBelowStartRevision(t *testing.T) {
	fc := &belowStartClient{streams: make(chan *belowStartStream, 4)}
	c := &Client{}
	c.lg.Store(zap.NewNop())
	w := NewWatchFromWatchClient(fc, c)
	defer w.Close()
	chc := make(chan WatchChan, 1)
	go func() { chc <- w.Watch(context.Background(), "a", WithRev(10), WithProgressNotify()) }()
	s1 := <-fc.streams
	if r := <-s1.sent; r.GetCreateRequest().GetStartRevision() != 10 {
		t.Fatalf("unexpected create %v", r)
	}
	s1.recv <- belowStartRecv{resp: &pb.WatchResponse{Header: &pb.ResponseHeader{Revision: 5}, WatchId: 0, Created: true}}
	wch := <-chc
	s1.recv <- belowStartRecv{resp: &pb.WatchResponse{Header: &pb.ResponseHeader{Revision: 5}, WatchId: 0}}
	select {
	case resp := <-wch:
		if !resp.IsProgressNotify() {
			t.Fatalf("expected a progress notification, got %v", resp)
		}
	case <-time.After(time.Second):
		t.Fatal("no progress notification")
	}
	s1.recv <- belowStartRecv{err: status.Error(codes.Unavailable, "transient")}
	s2 := <-fc.streams
	select {
	case r := <-s2.sent:
		if rev := r.GetCreateRequest().GetStartRevision(); rev < 10 {
			t.Fatalf("the watch from revision 10 resumed from revision %d", rev)
		}
	case <-time.After(time.Second):
		t.Fatal("no resumed create")
	}
}
