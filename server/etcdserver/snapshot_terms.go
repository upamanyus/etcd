// Copyright 2026 The etcd Authors
// SPDX-License-Identifier: Apache-2.0

package etcdserver

import "go.etcd.io/raft/v3/raftpb"

// A pre-3.5 backend can contain a consistent index without its term. Keep the
// terms above the apply cursor as they were at each Ready. Reading live Raft
// storage in the apply goroutine could race a later snapshot installation.
type replayTermRun struct {
	first, last, term uint64
}

type replayTerms []replayTermRun

// after discards entries that this batch has already handed to the applier.
func (r replayTerms) after(index uint64) replayTerms {
	for len(r) > 0 && r[0].last <= index {
		r = r[1:]
	}
	return r
}

// appendEntries replaces the suffix beginning at the first new entry. Old
// batches may still refer to r, including a range cut in the middle.
func (r replayTerms) appendEntries(ents []*raftpb.Entry) replayTerms {
	if len(ents) == 0 {
		return r
	}
	first := ents[0].GetIndex()
	n := len(r)
	for n > 0 && r[n-1].last >= first {
		n--
	}
	out := r[:n:n]
	if n < len(r) && r[n].first < first {
		out = append(out, replayTermRun{r[n].first, first - 1, r[n].term})
	}
	// Terms in a Raft log are monotone, so the usual same-term batch needs
	// only its first and last entries.
	for len(ents) > 0 {
		term := ents[0].GetTerm()
		count := len(ents)
		if ents[count-1].GetTerm() != term {
			count = 1
			for ents[count].GetTerm() == term {
				count++
			}
		}
		run := replayTermRun{ents[0].GetIndex(), ents[count-1].GetIndex(), term}
		if n := len(out); n > 0 && out[n-1].term == term {
			run.first = out[n-1].first
			out = out[: n-1 : n-1]
		}
		out = append(out, run)
		ents = ents[count:]
	}
	return out
}

// covers reports whether an index is in this Ready's retained log suffix.
func (r replayTerms) covers(index uint64) bool {
	return len(r) > 0 && r[0].first <= index && index <= r[len(r)-1].last
}

// termAt reads an index in this batch's log. The caller holds a current-term
// snapshot request from this member. Its elected log contains the committed
// backend prefix, even if the recovered HardState.Commit was lower.
func (r replayTerms) termAt(index uint64) uint64 {
	i := 0
	for r[i].last < index {
		i++
	}
	return r[i].term
}
