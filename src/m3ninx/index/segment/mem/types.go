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

package mem

import (
	re "regexp"

	"github.com/sniperkit/snk.fork.m3/src/m3ninx/doc"
	"github.com/sniperkit/snk.fork.m3/src/m3ninx/postings"
)

// termsDictionary is an internal interface for a mutable terms dictionary.
type termsDictionary interface {
	// Insert inserts the field with the given ID into the terms dictionary.
	Insert(field doc.Field, id postings.ID)

	// ContainsTerm returns a bool indicating whether the terms dictionary contains
	// the given term.
	ContainsTerm(field, term []byte) bool

	// MatchTerm returns the postings list corresponding to documents which match the
	// given field term exactly.
	MatchTerm(field, term []byte) postings.List

	// MatchRegexp returns the postings list corresponding to documents which match the
	// given egular expression.
	MatchRegexp(field, regexp []byte, compiled *re.Regexp) postings.List

	// Fields returns the list of known fields.
	Fields() [][]byte

	// Terms returns the list of known terms values for the given field.
	Terms(field []byte) [][]byte
}

// ReadableSegment is an internal interface for reading from a segment.
//
// NB(jeromefroe): Currently mockgen requires that interfaces with embedded interfaces be
// generated with reflection mode, but private interfaces can only be generated with file
// mode so we can't mock this interface if its private. Once mockgen supports mocking
// private interfaces which contain embedded interfaces we can make this interface private.
type ReadableSegment interface {
	// matchTerm returns the postings list of documents which match the given term exactly.
	matchTerm(field, term []byte) (postings.List, error)

	// matchRegexp returns the postings list of documents which match the given regular expression.
	matchRegexp(name, regexp []byte, compiled *re.Regexp) (postings.List, error)

	// getDoc returns the document associated with the given ID.
	getDoc(id postings.ID) (doc.Document, error)
}
