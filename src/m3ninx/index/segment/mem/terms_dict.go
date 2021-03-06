/*
Sniperkit-Bot
- Date: 2018-08-11 22:33:29.968631097 +0200 CEST m=+0.112171202
- Status: analyzed
*/

// Copyright (c) 2017 Uber Technologies, Inc.
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

package mem

import (
	re "regexp"
	"sync"

	"github.com/sniperkit/snk.fork.m3/src/m3ninx/doc"
	"github.com/sniperkit/snk.fork.m3/src/m3ninx/postings"
)

// termsDict is an in-memory terms dictionary. It maps fields to postings lists.
type termsDict struct {
	opts Options

	fields struct {
		sync.RWMutex
		*fieldsMap
	}
}

func newTermsDict(opts Options) termsDictionary {
	dict := &termsDict{
		opts: opts,
	}
	dict.fields.fieldsMap = newFieldsMap(opts.InitialCapacity())
	return dict
}

func (d *termsDict) Insert(field doc.Field, id postings.ID) {
	postingsMap := d.getOrAddName(field.Name)
	postingsMap.Add(field.Value, id)
}

func (d *termsDict) ContainsTerm(field, term []byte) bool {
	_, found := d.matchTerm(field, term)
	return found
}

func (d *termsDict) MatchTerm(field, term []byte) postings.List {
	pl, found := d.matchTerm(field, term)
	if !found {
		return d.opts.PostingsListPool().Get()
	}
	return pl
}

func (d *termsDict) Fields() [][]byte {
	d.fields.RLock()
	defer d.fields.RUnlock()
	fields := make([][]byte, 0, d.fields.Len())
	for _, entry := range d.fields.Iter() {
		fields = append(fields, entry.Key())
	}
	return fields
}

func (d *termsDict) Terms(field []byte) [][]byte {
	d.fields.RLock()
	defer d.fields.RUnlock()
	values, ok := d.fields.Get(field)
	if !ok {
		return nil
	}
	return values.Keys()
}

func (d *termsDict) matchTerm(field, term []byte) (postings.List, bool) {
	d.fields.RLock()
	postingsMap, ok := d.fields.Get(field)
	d.fields.RUnlock()
	if !ok {
		return nil, false
	}
	pl, ok := postingsMap.Get(term)
	if !ok {
		return nil, false
	}
	return pl, true
}

func (d *termsDict) MatchRegexp(
	field, regexp []byte,
	compiled *re.Regexp,
) postings.List {
	d.fields.RLock()
	postingsMap, ok := d.fields.Get(field)
	d.fields.RUnlock()
	if !ok {
		return d.opts.PostingsListPool().Get()
	}
	pl, ok := postingsMap.GetRegex(compiled)
	if !ok {
		return d.opts.PostingsListPool().Get()
	}
	return pl
}

func (d *termsDict) getOrAddName(name []byte) *concurrentPostingsMap {
	// Cheap read lock to see if it already exists.
	d.fields.RLock()
	postingsMap, ok := d.fields.Get(name)
	d.fields.RUnlock()
	if ok {
		return postingsMap
	}

	// Acquire write lock and create.
	d.fields.Lock()
	postingsMap, ok = d.fields.Get(name)

	// Check if it's been created since we last acquired the lock.
	if ok {
		d.fields.Unlock()
		return postingsMap
	}

	postingsMap = newConcurrentPostingsMap(d.opts)
	d.fields.SetUnsafe(name, postingsMap, fieldsMapSetUnsafeOptions{
		NoCopyKey:     true,
		NoFinalizeKey: true,
	})
	d.fields.Unlock()
	return postingsMap
}
