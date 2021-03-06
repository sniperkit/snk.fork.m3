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

package downsample

import (
	"context"
	"sync"
	"time"

	"github.com/uber-go/tally"

	"github.com/sniperkit/snk.fork.m3/src/query/models"
	"github.com/sniperkit/snk.fork.m3/src/query/storage"
	"github.com/sniperkit/snk.fork.m3/src/query/ts"
	"github.com/sniperkit/snk.fork.m3aggregator/aggregator/handler"
	"github.com/sniperkit/snk.fork.m3aggregator/aggregator/handler/writer"
	"github.com/sniperkit/snk.fork.m3metrics/metric/aggregated"
	"github.com/sniperkit/snk.fork.m3x/instrument"
	xsync "github.com/sniperkit/snk.fork.m3x/sync"
)

const (
	aggregationSuffixTag = "agg"
)

type downsamplerFlushHandler struct {
	sync.RWMutex
	storage                 storage.Storage
	encodedTagsIteratorPool *encodedTagsIteratorPool
	workerPool              xsync.WorkerPool
	instrumentOpts          instrument.Options
	metrics                 downsamplerFlushHandlerMetrics
}

type downsamplerFlushHandlerMetrics struct {
	flushSuccess tally.Counter
	flushErrors  tally.Counter
}

func newDownsamplerFlushHandlerMetrics(
	scope tally.Scope,
) downsamplerFlushHandlerMetrics {
	return downsamplerFlushHandlerMetrics{
		flushSuccess: scope.Counter("flush-success"),
		flushErrors:  scope.Counter("flush-errors"),
	}
}

func newDownsamplerFlushHandler(
	storage storage.Storage,
	encodedTagsIteratorPool *encodedTagsIteratorPool,
	workerPool xsync.WorkerPool,
	instrumentOpts instrument.Options,
) handler.Handler {
	scope := instrumentOpts.MetricsScope().SubScope("downsampler-flush-handler")
	return &downsamplerFlushHandler{
		storage:                 storage,
		encodedTagsIteratorPool: encodedTagsIteratorPool,
		workerPool:              workerPool,
		instrumentOpts:          instrumentOpts,
		metrics:                 newDownsamplerFlushHandlerMetrics(scope),
	}
}

func (h *downsamplerFlushHandler) NewWriter(
	scope tally.Scope,
) (writer.Writer, error) {
	return &downsamplerFlushHandlerWriter{
		ctx:     context.Background(),
		handler: h,
	}, nil
}

func (h *downsamplerFlushHandler) Close() {
}

type downsamplerFlushHandlerWriter struct {
	wg      sync.WaitGroup
	ctx     context.Context
	handler *downsamplerFlushHandler
}

func (w *downsamplerFlushHandlerWriter) Write(
	mp aggregated.ChunkedMetricWithStoragePolicy,
) error {
	w.wg.Add(1)
	w.handler.workerPool.Go(func() {
		defer w.wg.Done()

		logger := w.handler.instrumentOpts.Logger()

		iter := w.handler.encodedTagsIteratorPool.Get()
		iter.Reset(mp.ChunkedID.Data)

		expected := iter.NumTags()
		if len(mp.ChunkedID.Suffix) != 0 {
			expected++
		}

		// Add extra tag since we may need to add an aggregation suffix tag
		tags := make(models.Tags, expected+1)
		for iter.Next() {
			name, value := iter.Current()
			tags[string(name)] = string(value)
		}
		if len(mp.ChunkedID.Suffix) != 0 {
			tags[aggregationSuffixTag] = string(mp.ChunkedID.Suffix)
		}

		err := iter.Err()
		iter.Close()
		if err != nil {
			logger.Errorf("downsampler flush error preparing write: %v", err)
			w.handler.metrics.flushErrors.Inc(1)
			return
		}

		err = w.handler.storage.Write(w.ctx, &storage.WriteQuery{
			Tags: tags,
			Datapoints: ts.Datapoints{ts.Datapoint{
				Timestamp: time.Unix(0, mp.TimeNanos),
				Value:     mp.Value,
			}},
			Unit: mp.StoragePolicy.Resolution().Precision,
			Attributes: storage.Attributes{
				MetricsType: storage.AggregatedMetricsType,
				Retention:   mp.StoragePolicy.Retention().Duration(),
				Resolution:  mp.StoragePolicy.Resolution().Window,
			},
		})
		if err != nil {
			logger.Errorf("downsampler flush error failed write: %v", err)
			w.handler.metrics.flushErrors.Inc(1)
			return
		}

		w.handler.metrics.flushSuccess.Inc(1)
	})

	return nil
}

func (w *downsamplerFlushHandlerWriter) Flush() error {
	// NB(r): This is a just simply waiting for inflight requests
	// to complete since this flush handler isn't connection based.
	w.wg.Wait()
	return nil
}

func (w *downsamplerFlushHandlerWriter) Close() error {
	// NB(r): This is a no-op since this flush handler isn't connection based.
	return nil
}
