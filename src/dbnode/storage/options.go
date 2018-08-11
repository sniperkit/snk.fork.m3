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

package storage

import (
	"errors"
	"fmt"
	"io"
	"math"
	"runtime"
	"time"

	"github.com/sniperkit/snk.fork.m3/src/dbnode/clock"
	"github.com/sniperkit/snk.fork.m3/src/dbnode/encoding"
	"github.com/sniperkit/snk.fork.m3/src/dbnode/encoding/m3tsz"
	"github.com/sniperkit/snk.fork.m3/src/dbnode/persist"
	"github.com/sniperkit/snk.fork.m3/src/dbnode/persist/fs/commitlog"
	"github.com/sniperkit/snk.fork.m3/src/dbnode/retention"
	m3dbruntime "github.com/sniperkit/snk.fork.m3/src/dbnode/runtime"
	"github.com/sniperkit/snk.fork.m3/src/dbnode/storage/block"
	"github.com/sniperkit/snk.fork.m3/src/dbnode/storage/bootstrap"
	"github.com/sniperkit/snk.fork.m3/src/dbnode/storage/index"
	"github.com/sniperkit/snk.fork.m3/src/dbnode/storage/namespace"
	"github.com/sniperkit/snk.fork.m3/src/dbnode/storage/repair"
	"github.com/sniperkit/snk.fork.m3/src/dbnode/storage/series"
	"github.com/sniperkit/snk.fork.m3/src/dbnode/x/xcounter"
	"github.com/sniperkit/snk.fork.m3/src/dbnode/x/xio"
	"github.com/sniperkit/snk.fork.m3x/context"
	"github.com/sniperkit/snk.fork.m3x/ident"
	"github.com/sniperkit/snk.fork.m3x/instrument"
	"github.com/sniperkit/snk.fork.m3x/pool"
	xsync "github.com/sniperkit/snk.fork.m3x/sync"
)

const (
	// defaultBytesPoolBucketCapacity is the default bytes buffer capacity for the default bytes pool bucket
	defaultBytesPoolBucketCapacity = 256

	// defaultBytesPoolBucketCount is the default count of elements for the default bytes pool bucket
	defaultBytesPoolBucketCount = 4096

	// defaultRepairEnabled enables repair by default
	defaultRepairEnabled = true

	// defaultErrorWindowForLoad is the default error window for evaluating server load
	defaultErrorWindowForLoad = 10 * time.Second

	// defaultErrorThresholdForLoad is the default error threshold for considering server overloaded
	defaultErrorThresholdForLoad = 1000

	// defaultIndexingEnabled disables indexing by default
	defaultIndexingEnabled = false

	// defaultMinSnapshotInterval is the default minimum interval that must elapse between snapshots
	defaultMinSnapshotInterval = time.Minute
)

var (
	// defaultBootstrapProcessProvider is the default bootstrap provider for the database
	defaultBootstrapProcessProvider = bootstrap.NewNoOpProcessProvider()

	// defaultPoolOptions are the pool options used by default
	defaultPoolOptions pool.ObjectPoolOptions

	timeZero time.Time
)

var (
	errNamespaceInitializerNotSet = errors.New("namespace registry initializer not set")
	errRepairOptionsNotSet        = errors.New("repair enabled but repair options are not set")
	errIndexOptionsNotSet         = errors.New("index enabled but index options are not set")
	errPersistManagerNotSet       = errors.New("persist manager is not set")
)

// NewSeriesOptionsFromOptions creates a new set of database series options from provided options.
func NewSeriesOptionsFromOptions(opts Options, ropts retention.Options) series.Options {
	if ropts == nil {
		ropts = retention.NewOptions()
	}

	return opts.SeriesOptions().
		SetClockOptions(opts.ClockOptions()).
		SetInstrumentOptions(opts.InstrumentOptions()).
		SetRetentionOptions(ropts).
		SetDatabaseBlockOptions(opts.DatabaseBlockOptions()).
		SetCachePolicy(opts.SeriesCachePolicy()).
		SetContextPool(opts.ContextPool()).
		SetEncoderPool(opts.EncoderPool()).
		SetMultiReaderIteratorPool(opts.MultiReaderIteratorPool()).
		SetIdentifierPool(opts.IdentifierPool())
}

type options struct {
	clockOpts                      clock.Options
	instrumentOpts                 instrument.Options
	nsRegistryInitializer          namespace.Initializer
	blockOpts                      block.Options
	commitLogOpts                  commitlog.Options
	runtimeOptsMgr                 m3dbruntime.OptionsManager
	errCounterOpts                 xcounter.Options
	errWindowForLoad               time.Duration
	errThresholdForLoad            int64
	indexingEnabled                bool
	repairEnabled                  bool
	indexOpts                      index.Options
	repairOpts                     repair.Options
	newEncoderFn                   encoding.NewEncoderFn
	newDecoderFn                   encoding.NewDecoderFn
	bootstrapProcessProvider       bootstrap.ProcessProvider
	persistManager                 persist.Manager
	minSnapshotInterval            time.Duration
	blockRetrieverManager          block.DatabaseBlockRetrieverManager
	poolOpts                       pool.ObjectPoolOptions
	contextPool                    context.Pool
	seriesCachePolicy              series.CachePolicy
	seriesOpts                     series.Options
	seriesPool                     series.DatabaseSeriesPool
	bytesPool                      pool.CheckedBytesPool
	encoderPool                    encoding.EncoderPool
	segmentReaderPool              xio.SegmentReaderPool
	readerIteratorPool             encoding.ReaderIteratorPool
	multiReaderIteratorPool        encoding.MultiReaderIteratorPool
	identifierPool                 ident.Pool
	fetchBlockMetadataResultsPool  block.FetchBlockMetadataResultsPool
	fetchBlocksMetadataResultsPool block.FetchBlocksMetadataResultsPool
	queryIDsWorkerPool             xsync.WorkerPool
}

// NewOptions creates a new set of storage options with defaults
func NewOptions() Options {
	return newOptions(defaultPoolOptions)
}

func newOptions(poolOpts pool.ObjectPoolOptions) Options {
	bytesPool := pool.NewCheckedBytesPool(nil, poolOpts, func(s []pool.Bucket) pool.BytesPool {
		return pool.NewBytesPool(s, poolOpts)
	})
	bytesPool.Init()
	seriesOpts := series.NewOptions()

	// Default to using half of the available cores for querying IDs
	queryIDsWorkerPool := xsync.NewWorkerPool(int(math.Ceil(float64(runtime.NumCPU()) / 2)))
	queryIDsWorkerPool.Init()

	o := &options{
		clockOpts:                clock.NewOptions(),
		instrumentOpts:           instrument.NewOptions(),
		blockOpts:                block.NewOptions(),
		commitLogOpts:            commitlog.NewOptions(),
		runtimeOptsMgr:           m3dbruntime.NewOptionsManager(),
		errCounterOpts:           xcounter.NewOptions(),
		errWindowForLoad:         defaultErrorWindowForLoad,
		errThresholdForLoad:      defaultErrorThresholdForLoad,
		indexingEnabled:          defaultIndexingEnabled,
		indexOpts:                index.NewOptions(),
		repairEnabled:            defaultRepairEnabled,
		repairOpts:               repair.NewOptions(),
		bootstrapProcessProvider: defaultBootstrapProcessProvider,
		minSnapshotInterval:      defaultMinSnapshotInterval,
		poolOpts:                 poolOpts,
		contextPool: context.NewPool(context.NewOptions().
			SetContextPoolOptions(poolOpts).
			SetFinalizerPoolOptions(poolOpts)),
		seriesCachePolicy:       series.DefaultCachePolicy,
		seriesOpts:              seriesOpts,
		seriesPool:              series.NewDatabaseSeriesPool(poolOpts),
		bytesPool:               bytesPool,
		encoderPool:             encoding.NewEncoderPool(poolOpts),
		segmentReaderPool:       xio.NewSegmentReaderPool(poolOpts),
		readerIteratorPool:      encoding.NewReaderIteratorPool(poolOpts),
		multiReaderIteratorPool: encoding.NewMultiReaderIteratorPool(poolOpts),
		identifierPool: ident.NewPool(bytesPool, ident.PoolOptions{
			IDPoolOptions:           poolOpts,
			TagsPoolOptions:         poolOpts,
			TagsIteratorPoolOptions: poolOpts,
		}),
		fetchBlockMetadataResultsPool:  block.NewFetchBlockMetadataResultsPool(poolOpts, 0),
		fetchBlocksMetadataResultsPool: block.NewFetchBlocksMetadataResultsPool(poolOpts, 0),
		queryIDsWorkerPool:             queryIDsWorkerPool,
	}
	return o.SetEncodingM3TSZPooled()
}

func (o *options) Validate() error {
	// validate namespace registry
	init := o.NamespaceInitializer()
	if init == nil {
		return errNamespaceInitializerNotSet
	}

	// validate commit log options
	clOpts := o.CommitLogOptions()
	if err := clOpts.Validate(); err != nil {
		return fmt.Errorf("unable to validate commit log options: %v", err)
	}

	// validate repair options
	if o.RepairEnabled() {
		rOpts := o.RepairOptions()
		if rOpts == nil {
			return errRepairOptionsNotSet
		}
		if err := rOpts.Validate(); err != nil {
			return fmt.Errorf("unable to validate repair options, err: %v", err)
		}
	}

	// validate indexing options
	iOpts := o.IndexOptions()
	if iOpts == nil {
		return errIndexOptionsNotSet
	}
	if err := iOpts.Validate(); err != nil {
		return fmt.Errorf("unable to validate index options, err: %v", err)
	}

	// validate that persist manager is present, if not return
	// error if error occurred during default creation otherwise
	// it was set to nil by a caller
	if o.persistManager == nil {
		return errPersistManagerNotSet
	}

	// validate series cache policy
	return series.ValidateCachePolicy(o.seriesCachePolicy)
}

func (o *options) SetClockOptions(value clock.Options) Options {
	opts := *o
	opts.clockOpts = value
	opts.commitLogOpts = opts.commitLogOpts.SetClockOptions(value)
	opts.indexOpts = opts.indexOpts.SetClockOptions(value)
	opts.seriesOpts = NewSeriesOptionsFromOptions(&opts, nil)
	return &opts
}

func (o *options) ClockOptions() clock.Options {
	return o.clockOpts
}

func (o *options) SetInstrumentOptions(value instrument.Options) Options {
	opts := *o
	opts.instrumentOpts = value
	opts.commitLogOpts = opts.commitLogOpts.SetInstrumentOptions(value)
	opts.indexOpts = opts.indexOpts.SetInstrumentOptions(value)
	opts.seriesOpts = NewSeriesOptionsFromOptions(&opts, nil)
	return &opts
}

func (o *options) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

func (o *options) SetNamespaceInitializer(value namespace.Initializer) Options {
	opts := *o
	opts.nsRegistryInitializer = value
	return &opts
}

func (o *options) NamespaceInitializer() namespace.Initializer {
	return o.nsRegistryInitializer
}

func (o *options) SetDatabaseBlockOptions(value block.Options) Options {
	opts := *o
	opts.blockOpts = value
	opts.seriesOpts = NewSeriesOptionsFromOptions(&opts, nil)
	return &opts
}

func (o *options) DatabaseBlockOptions() block.Options {
	return o.blockOpts
}

func (o *options) SetCommitLogOptions(value commitlog.Options) Options {
	opts := *o
	opts.commitLogOpts = value
	return &opts
}

func (o *options) CommitLogOptions() commitlog.Options {
	return o.commitLogOpts
}

func (o *options) SetRuntimeOptionsManager(value m3dbruntime.OptionsManager) Options {
	opts := *o
	opts.runtimeOptsMgr = value
	return &opts
}

func (o *options) RuntimeOptionsManager() m3dbruntime.OptionsManager {
	return o.runtimeOptsMgr
}

func (o *options) SetErrorCounterOptions(value xcounter.Options) Options {
	opts := *o
	opts.errCounterOpts = value
	return &opts
}

func (o *options) ErrorCounterOptions() xcounter.Options {
	return o.errCounterOpts
}

func (o *options) SetErrorWindowForLoad(value time.Duration) Options {
	opts := *o
	opts.errWindowForLoad = value
	return &opts
}

func (o *options) ErrorWindowForLoad() time.Duration {
	return o.errWindowForLoad
}

func (o *options) SetErrorThresholdForLoad(value int64) Options {
	opts := *o
	opts.errThresholdForLoad = value
	return &opts
}

func (o *options) ErrorThresholdForLoad() int64 {
	return o.errThresholdForLoad
}

func (o *options) SetIndexOptions(value index.Options) Options {
	opts := *o
	opts.indexOpts = value
	return &opts
}

func (o *options) IndexOptions() index.Options {
	return o.indexOpts
}

func (o *options) SetRepairEnabled(b bool) Options {
	opts := *o
	opts.repairEnabled = b
	return &opts
}

func (o *options) RepairEnabled() bool {
	return o.repairEnabled
}

func (o *options) SetRepairOptions(value repair.Options) Options {
	opts := *o
	opts.repairOpts = value
	return &opts
}

func (o *options) RepairOptions() repair.Options {
	return o.repairOpts
}

func (o *options) SetEncodingM3TSZPooled() Options {
	opts := *o

	buckets := []pool.Bucket{{
		Capacity: defaultBytesPoolBucketCapacity,
		Count:    defaultBytesPoolBucketCount,
	}}
	newBackingBytesPool := func(s []pool.Bucket) pool.BytesPool {
		return pool.NewBytesPool(s, opts.poolOpts)
	}
	bytesPool := pool.NewCheckedBytesPool(buckets, o.poolOpts, newBackingBytesPool)
	bytesPool.Init()
	opts.bytesPool = bytesPool

	// initialize context pool
	opts.contextPool = context.NewPool(context.NewOptions().
		SetContextPoolOptions(opts.poolOpts).
		SetFinalizerPoolOptions(opts.poolOpts))

	encoderPool := encoding.NewEncoderPool(opts.poolOpts)
	readerIteratorPool := encoding.NewReaderIteratorPool(opts.poolOpts)

	// initialize segment reader pool
	segmentReaderPool := xio.NewSegmentReaderPool(opts.poolOpts)
	segmentReaderPool.Init()
	opts.segmentReaderPool = segmentReaderPool

	encodingOpts := encoding.NewOptions().
		SetBytesPool(bytesPool).
		SetEncoderPool(encoderPool).
		SetReaderIteratorPool(readerIteratorPool).
		SetSegmentReaderPool(segmentReaderPool)

	// initialize encoder pool
	encoderPool.Init(func() encoding.Encoder {
		return m3tsz.NewEncoder(timeZero, nil, m3tsz.DefaultIntOptimizationEnabled, encodingOpts)
	})
	opts.encoderPool = encoderPool

	// initialize single reader iterator pool
	readerIteratorPool.Init(func(r io.Reader) encoding.ReaderIterator {
		return m3tsz.NewReaderIterator(r, m3tsz.DefaultIntOptimizationEnabled, encodingOpts)
	})
	opts.readerIteratorPool = readerIteratorPool

	// initialize multi reader iterator pool
	multiReaderIteratorPool := encoding.NewMultiReaderIteratorPool(opts.poolOpts)
	multiReaderIteratorPool.Init(func(r io.Reader) encoding.ReaderIterator {
		return m3tsz.NewReaderIterator(r, m3tsz.DefaultIntOptimizationEnabled, encodingOpts)
	})
	opts.multiReaderIteratorPool = multiReaderIteratorPool

	opts.blockOpts = opts.blockOpts.
		SetEncoderPool(encoderPool).
		SetReaderIteratorPool(readerIteratorPool).
		SetMultiReaderIteratorPool(multiReaderIteratorPool).
		SetBytesPool(bytesPool)

	opts.seriesOpts = NewSeriesOptionsFromOptions(&opts, nil)
	return &opts
}

func (o *options) SetNewEncoderFn(value encoding.NewEncoderFn) Options {
	opts := *o
	opts.newEncoderFn = value
	return &opts
}

func (o *options) NewEncoderFn() encoding.NewEncoderFn {
	return o.newEncoderFn
}

func (o *options) SetNewDecoderFn(value encoding.NewDecoderFn) Options {
	opts := *o
	opts.newDecoderFn = value
	return &opts
}

func (o *options) NewDecoderFn() encoding.NewDecoderFn {
	return o.newDecoderFn
}

func (o *options) SetBootstrapProcessProvider(value bootstrap.ProcessProvider) Options {
	opts := *o
	opts.bootstrapProcessProvider = value
	return &opts
}

func (o *options) BootstrapProcessProvider() bootstrap.ProcessProvider {
	return o.bootstrapProcessProvider
}

func (o *options) SetPersistManager(value persist.Manager) Options {
	opts := *o
	opts.persistManager = value
	return &opts
}

func (o *options) PersistManager() persist.Manager {
	return o.persistManager
}

func (o *options) SetDatabaseBlockRetrieverManager(value block.DatabaseBlockRetrieverManager) Options {
	opts := *o
	opts.blockRetrieverManager = value
	return &opts
}

func (o *options) DatabaseBlockRetrieverManager() block.DatabaseBlockRetrieverManager {
	return o.blockRetrieverManager
}

func (o *options) SetContextPool(value context.Pool) Options {
	opts := *o
	opts.contextPool = value
	return &opts
}

func (o *options) ContextPool() context.Pool {
	return o.contextPool
}

func (o *options) SetSeriesCachePolicy(value series.CachePolicy) Options {
	opts := *o
	opts.seriesCachePolicy = value
	return &opts
}

func (o *options) SeriesCachePolicy() series.CachePolicy {
	return o.seriesCachePolicy
}

func (o *options) SetSeriesOptions(value series.Options) Options {
	opts := *o
	opts.seriesOpts = value
	return &opts
}

func (o *options) SeriesOptions() series.Options {
	return o.seriesOpts
}

func (o *options) SetDatabaseSeriesPool(value series.DatabaseSeriesPool) Options {
	opts := *o
	opts.seriesPool = value
	return &opts
}

func (o *options) DatabaseSeriesPool() series.DatabaseSeriesPool {
	return o.seriesPool
}

func (o *options) SetBytesPool(value pool.CheckedBytesPool) Options {
	opts := *o
	opts.bytesPool = value
	return &opts
}

func (o *options) BytesPool() pool.CheckedBytesPool {
	return o.bytesPool
}

func (o *options) SetEncoderPool(value encoding.EncoderPool) Options {
	opts := *o
	opts.encoderPool = value
	return &opts
}

func (o *options) EncoderPool() encoding.EncoderPool {
	return o.encoderPool
}

func (o *options) SetSegmentReaderPool(value xio.SegmentReaderPool) Options {
	opts := *o
	opts.segmentReaderPool = value
	return &opts
}

func (o *options) SegmentReaderPool() xio.SegmentReaderPool {
	return o.segmentReaderPool
}

func (o *options) SetReaderIteratorPool(value encoding.ReaderIteratorPool) Options {
	opts := *o
	opts.readerIteratorPool = value
	return &opts
}

func (o *options) ReaderIteratorPool() encoding.ReaderIteratorPool {
	return o.readerIteratorPool
}

func (o *options) SetMultiReaderIteratorPool(value encoding.MultiReaderIteratorPool) Options {
	opts := *o
	opts.multiReaderIteratorPool = value
	return &opts
}

func (o *options) MultiReaderIteratorPool() encoding.MultiReaderIteratorPool {
	return o.multiReaderIteratorPool
}

func (o *options) SetIdentifierPool(value ident.Pool) Options {
	opts := *o
	opts.indexOpts = opts.indexOpts.SetIdentifierPool(value)
	opts.identifierPool = value
	return &opts
}

func (o *options) IdentifierPool() ident.Pool {
	return o.identifierPool
}

func (o *options) SetFetchBlockMetadataResultsPool(value block.FetchBlockMetadataResultsPool) Options {
	opts := *o
	opts.fetchBlockMetadataResultsPool = value
	return &opts
}

func (o *options) FetchBlockMetadataResultsPool() block.FetchBlockMetadataResultsPool {
	return o.fetchBlockMetadataResultsPool
}

func (o *options) SetFetchBlocksMetadataResultsPool(value block.FetchBlocksMetadataResultsPool) Options {
	opts := *o
	opts.fetchBlocksMetadataResultsPool = value
	return &opts
}

func (o *options) FetchBlocksMetadataResultsPool() block.FetchBlocksMetadataResultsPool {
	return o.fetchBlocksMetadataResultsPool
}

func (o *options) SetMinimumSnapshotInterval(value time.Duration) Options {
	opts := *o
	opts.minSnapshotInterval = value
	return &opts
}

func (o *options) MinimumSnapshotInterval() time.Duration {
	return o.minSnapshotInterval
}

func (o *options) SetQueryIDsWorkerPool(value xsync.WorkerPool) Options {
	opts := *o
	opts.queryIDsWorkerPool = value
	return &opts
}

func (o *options) QueryIDsWorkerPool() xsync.WorkerPool {
	return o.queryIDsWorkerPool
}
