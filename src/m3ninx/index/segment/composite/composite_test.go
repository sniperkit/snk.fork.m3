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

package composite

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/m3db/m3/src/m3ninx/doc"
	sgmt "github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3/src/m3ninx/index/segment/mem"
	"github.com/m3db/m3/src/m3ninx/index/util"
	"github.com/m3db/m3/src/m3ninx/postings"

	"github.com/stretchr/testify/require"
)

var (
	testOptions = NewOptions()

	fewTestDocuments = []doc.Document{
		doc.Document{
			Fields: []doc.Field{
				doc.Field{
					Name:  []byte("fruit"),
					Value: []byte("banana"),
				},
				doc.Field{
					Name:  []byte("color"),
					Value: []byte("yellow"),
				},
			},
		},
		doc.Document{
			Fields: []doc.Field{
				doc.Field{
					Name:  []byte("fruit"),
					Value: []byte("apple"),
				},
				doc.Field{
					Name:  []byte("color"),
					Value: []byte("red"),
				},
			},
		},
		doc.Document{
			ID: []byte("42"),
			Fields: []doc.Field{
				doc.Field{
					Name:  []byte("fruit"),
					Value: []byte("pineapple"),
				},
				doc.Field{
					Name:  []byte("color"),
					Value: []byte("yellow"),
				},
			},
		},
	}
	lotsTestDocuments = util.MustReadDocs("../../util/testdata/node_exporter.json", 2000)

	testDocuments = []struct {
		name string
		docs []doc.Document
	}{
		{
			name: "few documents",
			docs: fewTestDocuments,
		},
		{
			name: "many documents",
			docs: lotsTestDocuments,
		},
	}
)

func TestCompositeSegmentSize(t *testing.T) {
	for _, td := range testDocuments {
		t.Run(td.name, func(t *testing.T) {
			memSeg := newTestMemSegment(t, td.docs)
			for i := 1; i < 5; i++ {
				testSeg := newTestCompositeSegment(t, i, td.docs)
				require.Equal(t, memSeg.Size(), testSeg.Size())
			}
		})
	}
}

func TestFieldsEquals(t *testing.T) {
	for _, td := range testDocuments {
		for numSplits := 1; numSplits < 5; numSplits++ {
			t.Run(fmt.Sprintf("%s/numSplits=%d", td.name, numSplits), func(t *testing.T) {
				memSeg := newTestMemSegment(t, td.docs)
				memFieldsIter, err := memSeg.Fields()
				require.NoError(t, err)
				memFields := toSlice(t, memFieldsIter)

				compSeg := newTestCompositeSegment(t, numSplits, td.docs)
				compFieldsIter, err := compSeg.Fields()
				require.NoError(t, err)
				compFields := toSlice(t, compFieldsIter)
				assertSliceOfByteSlicesEqual(t, memFields, compFields)
			})
		}
	}
}

func assertSliceOfByteSlicesEqual(t *testing.T, a, b [][]byte) {
	require.Equal(t, len(a), len(b), fmt.Sprintf("a = [%s], b = [%s]", pprint(a), pprint(b)))
	require.Equal(t, a, b)
}

func pprint(a [][]byte) string {
	var buf bytes.Buffer
	for i, t := range a {
		if i != 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(fmt.Sprintf("%d %s", i, string(t)))
	}
	return buf.String()
}

func newTestCompositeSegment(t *testing.T, numSegments int, docs []doc.Document) sgmt.Segment {
	splits := make([][]doc.Document, numSegments)
	for i := 0; i < len(docs); i++ {
		idx := i % numSegments
		splits[idx] = append(splits[idx], docs[i])
	}
	segs := make([]sgmt.Segment, 0, numSegments)
	for i := 0; i < numSegments; i++ {
		seg := newTestMemSegment(t, splits[i])
		segs = append(segs, seg)
	}
	c, err := NewSegment(testOptions, segs...)
	require.NoError(t, err)
	return c
}

func newTestMemSegment(t *testing.T, docs []doc.Document) sgmt.Segment {
	opts := mem.NewOptions()
	s, err := mem.NewSegment(postings.ID(0), opts)
	require.NoError(t, err)
	for _, d := range docs {
		_, err := s.Insert(d)
		require.NoError(t, err)
	}
	_, err = s.Seal()
	require.NoError(t, err)
	return s
}
