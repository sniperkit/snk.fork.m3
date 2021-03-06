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

package pilosa

import (
	"bytes"

	"github.com/pilosa/pilosa/roaring"

	"github.com/sniperkit/snk.fork.m3/src/m3ninx/postings"
)

// Encoder helps serialize a Pilosa RoaringBitmap
type Encoder struct {
	scratchBuffer bytes.Buffer
}

// NewEncoder returns a new Encoder.
func NewEncoder() *Encoder {
	return &Encoder{}
}

// Reset resets the internal state of the encoder to allow
// for re-use.
func (e *Encoder) Reset() {
	e.scratchBuffer.Reset()
}

// Encode encodes the provided postings list in serialized form.
// The bytes returned are invalidate on a subsequent call to Encode(),
// or Reset().
func (e *Encoder) Encode(pl postings.List) ([]byte, error) {
	e.scratchBuffer.Reset()

	pilosaBitmap, err := toPilosa(pl)
	if err != nil {
		return nil, err
	}

	_, err = pilosaBitmap.WriteTo(&e.scratchBuffer)
	if err != nil {
		return nil, err
	}

	return e.scratchBuffer.Bytes(), nil
}

func toPilosa(pl postings.List) (*roaring.Bitmap, error) {
	bitmap := roaring.NewBitmap()
	iter := pl.Iterator()

	for iter.Next() {
		_, err := bitmap.Add(uint64(iter.Current()))
		if err != nil {
			return nil, err
		}
	}

	if err := iter.Err(); err != nil {
		return nil, err
	}

	return bitmap, nil
}

// Unmarshal unmarshals the provided bytes into a postings.List.
func Unmarshal(data []byte, allocFn postings.PoolAllocateFn) (postings.List, error) {
	b := roaring.NewBitmap()
	if err := b.UnmarshalBinary(data); err != nil {
		return nil, err
	}
	pl := allocFn()
	iter := NewIterator(b.Iterator())
	return pl, pl.AddIterator(iter)
}
