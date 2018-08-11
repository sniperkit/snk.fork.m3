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

package client

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sniperkit/snk.fork.m3x/pool"
)

func TestFetchStatePoolInvalidInteraction(t *testing.T) {
	p := newFetchStatePool(pool.NewObjectPoolOptions().SetSize(1))
	p.Init()
	s := p.Get()

	testPool := &testFetchStatePool{t, s, false}
	s.pool = testPool

	s.op = newFetchTaggedOp(nil)
	s.incRef()
	require.Panics(t, func() {
		s.decRef()
	})
}

func TestFetchStatePoolValidInteraction(t *testing.T) {
	p := newFetchStatePool(pool.NewObjectPoolOptions().SetSize(1))
	p.Init()
	s := p.Get()

	testPool := &testFetchStatePool{t, s, false}
	s.pool = testPool

	s.op = newFetchTaggedOp(nil)
	s.op.incRef()
	s.incRef()
	s.decRef()
	require.True(t, testPool.called)
	require.Nil(t, s.op)
}

type testFetchStatePool struct {
	t             *testing.T
	expectedState *fetchState
	called        bool
}

var _ fetchStatePool = &testFetchStatePool{}

func (p *testFetchStatePool) Put(o *fetchState) {
	require.False(p.t, p.called)
	p.called = true
	require.Equal(p.t, p.expectedState, o)
}

func (p *testFetchStatePool) Init()            { panic("not implemented") }
func (p *testFetchStatePool) Get() *fetchState { panic("not implemented") }
