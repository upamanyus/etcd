// Copyright 2016 The etcd Authors
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

package recipe

import (
	"context"

	"go.etcd.io/etcd/api/v3/mvccpb"
	v3 "go.etcd.io/etcd/client/v3"
)

// Queue implements a multi-reader, multi-writer distributed queue.
type Queue struct {
	client *v3.Client
	ctx    context.Context

	keyPrefix string
}

func NewQueue(client *v3.Client, keyPrefix string) *Queue {
	return &Queue{client, context.TODO(), keyPrefix}
}

func (q *Queue) Enqueue(val string) error {
	_, err := newUniqueKV(q.client.KV, q.keyPrefix, val)
	return err
}

// Dequeue returns Enqueue()'d elements in FIFO order. If the
// queue is empty, Dequeue blocks until elements are available.
func (q *Queue) Dequeue() (string, error) {
	// TODO: fewer round trips by fetching more than one key
	resp, err := q.client.KV.Get(q.ctx, q.keyPrefix, v3.WithFirstRev()...)
	if err != nil {
		return "", err
	}

	kv, err := claimFirstKey(q.client.KV, resp.Kvs)
	if err != nil {
		return "", err
	} else if kv != nil {
		return string(kv.Value), nil
	} else if resp.More {
		// missed some items, retry to read in more
		return q.Dequeue()
	}

	// nothing yet; wait on elements
	evc, err := WaitPrefixEvents(
		q.client,
		q.keyPrefix,
		resp.Header.Revision,
		[]mvccpb.Event_EventType{mvccpb.Event_PUT})
	if err != nil {
		return "", err
	}

	// Gooseable: mvccpb.Event rather than its alias clientv3.Event (the same type):
	// Goose does not resolve a type alias when it selects a field through a pointer.
	var ev *mvccpb.Event = evc
	ok, err := deleteRevKey(q.client.KV, string(ev.Kv.Key), ev.Kv.ModRevision)
	if err != nil {
		return "", err
	} else if !ok {
		return q.Dequeue()
	}
	return string(ev.Kv.Value), err
}
