/*
Sniperkit-Bot
- Date: 2018-08-11 22:33:29.968631097 +0200 CEST m=+0.112171202
- Status: analyzed
*/

// Copyright (c) 2018 Uber Technologies, Inc.
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

package index

import (
	"errors"

	"github.com/sniperkit/snk.fork.m3/src/m3ninx/doc"
	"github.com/sniperkit/snk.fork.m3x/ident"
	"github.com/sniperkit/snk.fork.m3x/pool"
)

var (
	errUnableToAddDocMissingID = errors.New("corrupt data, unable to extract id")
)

type results struct {
	nsID       ident.ID
	size       int
	resultsMap *ResultsMap

	idPool    ident.Pool
	bytesPool pool.CheckedBytesPool

	pool ResultsPool
}

// NewResults returns a new results object.
func NewResults(opts Options) Results {
	return &results{
		resultsMap: newResultsMap(opts.IdentifierPool()),
		idPool:     opts.IdentifierPool(),
		bytesPool:  opts.CheckedBytesPool(),
		pool:       opts.ResultsPool(),
	}
}

func (r *results) Add(d doc.Document) (added bool, size int, err error) {
	added = false
	if len(d.ID) == 0 {
		return added, r.size, errUnableToAddDocMissingID
	}

	// NB: can cast the []byte -> ident.ID to avoid an alloc
	// before we're sure we need it.
	tsID := ident.BytesID(d.ID)

	// check if it already exists in the map.
	if r.resultsMap.Contains(tsID) {
		return added, r.size, nil
	}

	// i.e. it doesn't exist in the map, so we create the tags wrapping
	// fields prodided by the document.
	tags := r.tags(d.Fields)

	// We use Set() instead of SetUnsafe to ensure we're taking a copy of
	// the tsID's bytes.
	r.resultsMap.Set(tsID, tags)
	r.size++

	added = true
	return added, r.size, nil
}

func (r *results) tags(fields doc.Fields) ident.Tags {
	tags := r.idPool.Tags()
	for _, f := range fields {
		tags.Append(ident.Tag{
			Name:  r.copyBytes(f.Name),
			Value: r.copyBytes(f.Value),
		})
	}
	return tags
}

// copyBytes copies the provided bytes into an ident.ID backed by pooled types.
func (r *results) copyBytes(b []byte) ident.ID {
	cb := r.bytesPool.Get(len(b))
	cb.IncRef()
	cb.AppendAll(b)
	id := r.idPool.BinaryID(cb)
	// release held reference so now the only reference to the bytes is owned by `id`
	cb.DecRef()
	return id
}

func (r *results) Namespace() ident.ID {
	return r.nsID
}

func (r *results) Map() *ResultsMap {
	return r.resultsMap
}

func (r *results) Size() int {
	return r.size
}

func (r *results) Reset(nsID ident.ID) {
	// finalize existing held nsID
	if r.nsID != nil {
		r.nsID.Finalize()
	}
	// make an independent copy of the new nsID
	if nsID != nil {
		nsID = r.idPool.Clone(nsID)
	}
	r.nsID = nsID

	// reset all values from map first
	for _, entry := range r.resultsMap.Iter() {
		tags := entry.Value()
		tags.Finalize()
	}

	// reset all keys in the map next
	r.resultsMap.Reset()
	r.size = 0

	// NB: could do keys+value in one step but I'm trying to avoid
	// using an internal method of a code-gen'd type.
}

func (r *results) Finalize() {
	r.Reset(nil)

	if r.pool == nil {
		return
	}
	r.pool.Put(r)
}
