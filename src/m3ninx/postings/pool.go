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

package postings

import (
	xpool "github.com/sniperkit/snk.fork.m3x/pool"
)

type pool struct {
	pool xpool.ObjectPool
}

// PoolAllocateFn returns a new MutableList.
type PoolAllocateFn func() MutableList

// NewPool returns a new Pool.
func NewPool(
	opts xpool.ObjectPoolOptions,
	allocator PoolAllocateFn,
) Pool {
	p := &pool{
		pool: xpool.NewObjectPool(opts),
	}
	p.pool.Init(func() interface{} {
		return allocator()
	})
	return p
}

func (p *pool) Get() MutableList {
	return p.pool.Get().(MutableList)
}

func (p *pool) Put(pl MutableList) {
	pl.Reset()
	p.pool.Put(pl)
}
