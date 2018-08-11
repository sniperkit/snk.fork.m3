/*
Sniperkit-Bot
- Date: 2018-08-11 22:33:29.968631097 +0200 CEST m=+0.112171202
- Status: analyzed
*/

// Copyright (c) 2016 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package encoding

import (
	"time"

	"github.com/sniperkit/snk.fork.m3/src/dbnode/ts"
	"github.com/sniperkit/snk.fork.m3x/ident"
	xtime "github.com/sniperkit/snk.fork.m3x/time"
)

type seriesIterator struct {
	id               ident.ID
	nsID             ident.ID
	tags             ident.TagIterator
	start            time.Time
	end              time.Time
	iters            iterators
	multiReaderIters []MultiReaderIterator
	err              error
	firstNext        bool
	closed           bool
	pool             SeriesIteratorPool
}

// NewSeriesIterator creates a new series iterator.
// NB: The returned SeriesIterator assumes ownership of the provided `ident.ID`.
func NewSeriesIterator(
	id ident.ID,
	nsID ident.ID,
	tags ident.TagIterator,
	startInclusive, endExclusive time.Time,
	replicas []MultiReaderIterator,
	pool SeriesIteratorPool,
) SeriesIterator {
	it := &seriesIterator{pool: pool}
	it.Reset(id, nsID, tags, startInclusive, endExclusive, replicas)
	return it
}

func (it *seriesIterator) ID() ident.ID {
	return it.id
}

func (it *seriesIterator) Namespace() ident.ID {
	return it.nsID
}

func (it *seriesIterator) Tags() ident.TagIterator {
	return it.tags
}

func (it *seriesIterator) Start() time.Time {
	return it.start
}

func (it *seriesIterator) End() time.Time {
	return it.end
}

func (it *seriesIterator) Next() bool {
	if !it.firstNext {
		if !it.hasNext() {
			return false
		}
		it.moveToNext()
	}
	it.firstNext = false
	return it.hasNext()
}

func (it *seriesIterator) Current() (ts.Datapoint, xtime.Unit, ts.Annotation) {
	return it.iters.current()
}

func (it *seriesIterator) Err() error {
	return it.err
}

func (it *seriesIterator) Close() {
	if it.isClosed() {
		return
	}
	it.closed = true
	it.id.Finalize()
	it.nsID.Finalize()
	if it.tags != nil {
		it.tags.Close()
	}

	for idx := range it.multiReaderIters {
		it.multiReaderIters[idx] = nil
	}

	it.iters.reset()
	if it.pool != nil {
		it.pool.Put(it)
	}
}

func (it *seriesIterator) Replicas() []MultiReaderIterator {
	return it.multiReaderIters
}

func (it *seriesIterator) Reset(id ident.ID, nsID ident.ID, tags ident.TagIterator, startInclusive, endExclusive time.Time, replicas []MultiReaderIterator) {
	it.id = id
	it.nsID = nsID
	it.tags = tags
	it.start = startInclusive
	it.end = endExclusive
	it.iters.reset()
	it.iters.setFilter(startInclusive, endExclusive)
	it.multiReaderIters = it.multiReaderIters[:0]
	it.err = nil
	it.firstNext = true
	it.closed = false
	for _, replica := range replicas {
		if !replica.Next() || !it.iters.push(replica) {
			replica.Close()
			continue
		}
		it.multiReaderIters = append(it.multiReaderIters, replica)
	}
}

func (it *seriesIterator) hasError() bool {
	return it.err != nil
}

func (it *seriesIterator) isClosed() bool {
	return it.closed
}

func (it *seriesIterator) hasMore() bool {
	return it.iters.len() > 0
}

func (it *seriesIterator) hasNext() bool {
	return !it.hasError() && !it.isClosed() && it.hasMore()
}

func (it *seriesIterator) moveToNext() {
	for {
		prev := it.iters.at()
		next, err := it.iters.moveToValidNext()
		if err != nil {
			it.err = err
			return
		}
		if !next {
			return
		}

		curr := it.iters.at()
		if !curr.Equal(prev) {
			return
		}

		// Dedupe by continuing
	}
}
