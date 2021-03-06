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

package fs

import (
	"github.com/sniperkit/snk.fork.m3/src/dbnode/persist"
	"github.com/sniperkit/snk.fork.m3/src/dbnode/persist/fs"
	"github.com/sniperkit/snk.fork.m3/src/dbnode/runtime"
	"github.com/sniperkit/snk.fork.m3/src/dbnode/storage/block"
	"github.com/sniperkit/snk.fork.m3/src/dbnode/storage/bootstrap/result"
	"github.com/sniperkit/snk.fork.m3x/ident"
	"github.com/sniperkit/snk.fork.m3x/instrument"
)

// Options represents the options for bootstrapping from the filesystem.
type Options interface {
	// Validate validates the options are correct
	Validate() error

	// SetInstrumentOptions sets the instrumentation options
	SetInstrumentOptions(value instrument.Options) Options

	// InstrumentOptions returns the instrumentation options
	InstrumentOptions() instrument.Options

	// SetResultOptions sets the instrumentation options.
	SetResultOptions(value result.Options) Options

	// ResultOptions returns the instrumentation options.
	ResultOptions() result.Options

	// SetFilesystemOptions sets the filesystem options.
	SetFilesystemOptions(value fs.Options) Options

	// FilesystemOptions returns the filesystem options.
	FilesystemOptions() fs.Options

	// SetPersistManager sets the persistence manager used to flush blocks
	// when performing an incremental bootstrap run.
	SetPersistManager(value persist.Manager) Options

	// PersistManager returns the persistence manager used to flush blocks
	// when performing an incremental bootstrap run.
	PersistManager() persist.Manager

	// SetBoostrapDataNumProcessors sets the number of processors for CPU-bound
	// work for bootstrapping data file sets.
	SetBoostrapDataNumProcessors(value int) Options

	// BoostrapDataNumProcessors returns the number of processors for CPU-bound
	// work for bootstrapping data file sets.
	BoostrapDataNumProcessors() int

	// SetBoostrapIndexNumProcessors sets the number of processors for CPU-bound
	// work for bootstrapping data file sets.
	SetBoostrapIndexNumProcessors(value int) Options

	// BoostrapIndexNumProcessors returns the number of processors for CPU-bound
	// work for bootstrapping data file sets.
	BoostrapIndexNumProcessors() int

	// SetDatabaseBlockRetrieverManager sets the block retriever manager to
	// use when bootstrapping retrievable blocks instead of blocks
	// containing data.
	// If you don't wish to bootstrap retrievable blocks instead of
	// blocks containing data then do not set this manager.
	// You can opt into which namespace you wish to have this enabled for
	// by returning nil instead of a result when creating a new block retriever
	// for a namespace from the manager.
	SetDatabaseBlockRetrieverManager(
		value block.DatabaseBlockRetrieverManager,
	) Options

	// NewBlockRetrieverFn returns the new block retriever constructor to
	// use when bootstrapping retrievable blocks instead of blocks
	// containing data.
	DatabaseBlockRetrieverManager() block.DatabaseBlockRetrieverManager

	// SetRuntimeOptionsManager sets the runtime options manager.
	SetRuntimeOptionsManager(value runtime.OptionsManager) Options

	// RuntimeOptionsManager returns the runtime options manager.
	RuntimeOptionsManager() runtime.OptionsManager

	// SetIdentifierPool sets the identifier pool.
	SetIdentifierPool(value ident.Pool) Options

	// IdentifierPool returns the identifier pool.
	IdentifierPool() ident.Pool
}
