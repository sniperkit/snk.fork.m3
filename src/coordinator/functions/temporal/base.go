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

package temporal

import (
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/m3db/m3db/src/coordinator/block"
	"github.com/m3db/m3db/src/coordinator/executor/transform"
	"github.com/m3db/m3db/src/coordinator/parser"
)

var emptyOp = baseOp{}

// baseOp stores required properties for logical operations
type baseOp struct {
	operatorType string
	duration     time.Duration
	processorFn  MakeProcessor
}

func newBaseOp(args []interface{}, operatorType string, processorFn MakeProcessor) (baseOp, error) {
	if len(args) != 1 {
		return emptyOp, fmt.Errorf("invalid number of args for %s: %d", operatorType, len(args))
	}

	duration, ok := args[0].(time.Duration)
	if !ok {
		return emptyOp, fmt.Errorf("unable to cast to scalar argument: %v", args[0])
	}

	return baseOp{
		operatorType: operatorType,
		processorFn:  processorFn,
		duration:     duration,
	}, nil
}

// OpType for the operator
func (o baseOp) OpType() string {
	return o.operatorType
}

// String representation
func (o baseOp) String() string {
	return fmt.Sprintf("type: %s, duration: %v", o.OpType(), o.duration)
}

// Node creates an execution node
func (o baseOp) Node(controller *transform.Controller, opts transform.Options) transform.OpNode {
	return &baseNode{
		controller:    controller,
		cache:         transform.NewTimeCache(),
		op:            o,
		processor:     o.processorFn(o, controller),
		transformOpts: opts,
	}
}

// baseNode is an execution node
type baseNode struct {
	op            baseOp
	controller    *transform.Controller
	cache         *transform.TimeCache
	processor     Processor
	mu            sync.Mutex
	transformOpts transform.Options
}

// Process processes a block. The processing steps are as follows:
// 1. Figure out the maximum blocks needed for the temporal function
// 2. For the current block, figure out whether we have enough previous blocks which can help process it
// 3. For the blocks after current block, figure out which can be processed right now
// 4. Process all valid blocks from #3, #4 and mark them as processed
// 5. Run a sweep face to free up blocks which are no longer needed to be cached
func (c *baseNode) Process(ID parser.NodeID, b block.Block) error {
	iter, err := b.StepIter()
	if err != nil {
		return err
	}

	meta := iter.Meta()
	bounds := meta.Bounds
	blockDuration := bounds.Duration
	maxBlocks := int(math.Ceil(float64(c.op.duration) / float64(blockDuration)))

	leftRangeStart := bounds.Previous(maxBlocks).Start
	queryStartBounds := bounds.Nearest(c.transformOpts.TimeSpec.Start)

	if leftRangeStart.Before(queryStartBounds.Start) {
		leftRangeStart = queryStartBounds.Start
	}

	rightRangeStart := bounds.Next(maxBlocks).Start
	queryEndBounds := bounds.Nearest(c.transformOpts.TimeSpec.End.Add(-1 * bounds.StepSize))

	if rightRangeStart.After(queryEndBounds.Start) {
		rightRangeStart = queryEndBounds.Start
	}

	leftBlks, emptyLeftBlocks := c.processLeft(b, bounds, maxBlocks, leftRangeStart)

	processRequests := make([]processRequest, 0, len(leftBlks))
	if !emptyLeftBlocks {
		processRequests = append(processRequests, processRequest{blk: b, deps: leftBlks, bounds: bounds})
	}

	leftBlks = append(leftBlks, b)

	rightBlks, emptyRightBlocks := c.processRight(b, bounds, maxBlocks, rightRangeStart)

	for i := 0; i < len(rightBlks); i++ {
		lStart := maxBlocks - i
		if lStart > len(leftBlks) {
			continue
		}

		deps := leftBlks[len(leftBlks)-lStart:]
		deps = append(deps, rightBlks[:i]...)
		processRequests = append(processRequests, processRequest{blk: rightBlks[i], deps: deps, bounds: bounds.Next(i + 1)})

	}

	if emptyLeftBlocks || emptyRightBlocks {
		if err := c.cache.Add(bounds.Start, b); err != nil {
			return err
		}
	}

	return c.processCompletedBlocks(processRequests, queryStartBounds, queryEndBounds, maxBlocks)
}

func (c *baseNode) processLeft(b block.Block, bounds block.Bounds, maxBlocks int, leftRangeStart time.Time) ([]block.Block, bool) {
	leftRangeTimes := make([]time.Time, 0, maxBlocks)
	for t := leftRangeStart; t.Before(bounds.Start); t = t.Add(bounds.Duration) {
		leftRangeTimes = append(leftRangeTimes, t)
	}

	leftBlks := c.cache.MultiGet(leftRangeTimes)
	lastNil := lastEmpty(leftBlks)
	if lastNil >= 0 {
		return leftBlks[lastNil:], true
	}

	return leftBlks, false
}

func (c *baseNode) processRight(b block.Block, bounds block.Bounds, maxBlocks int, rightRangeStart time.Time) ([]block.Block, bool) {
	rightRangeTimes := make([]time.Time, 0, maxBlocks)
	for t := bounds.End(); !t.After(rightRangeStart); t = t.Add(bounds.Duration) {
		rightRangeTimes = append(rightRangeTimes, t)
	}

	rightBlks := c.cache.MultiGet(rightRangeTimes)
	firstNil := firstEmpty(rightBlks)
	return rightBlks[:firstNil], firstNil != len(rightBlks)
}

func (c *baseNode) processCompletedBlocks(processRequests []processRequest, queryStartBounds, queryEndBounds block.Bounds, maxBlocks int) error {
	processedKeys := make([]time.Time, len(processRequests))
	for i, req := range processRequests {
		if err := c.processSingleRequest(req); err != nil {
			return err
		}

		processedKeys[i] = req.bounds.Start
	}

	// Mark all blocks as processed
	c.cache.MarkProcessed(processedKeys)
	// Sweep to free blocks from cache with no dependencies
	c.sweep(c.cache.Processed(), queryStartBounds, queryEndBounds, maxBlocks)
	return nil
}

func (c *baseNode) processSingleRequest(request processRequest) error {
	aggDuration := c.op.duration
	seriesIter, err := request.blk.SeriesIter()
	if err != nil {
		return err
	}

	depIters := make([]block.SeriesIter, len(request.deps))
	for i, blk := range request.deps {
		iter, err := blk.SeriesIter()
		if err != nil {
			return err
		}
		depIters[i] = iter
	}

	bounds := seriesIter.Meta().Bounds
	steps := int((aggDuration + bounds.Duration) / bounds.StepSize)
	values := make([]float64, 0, steps)

	seriesMeta := seriesIter.SeriesMeta()
	resultSeriesMeta := make([]block.SeriesMeta, len(seriesMeta))
	for i, m := range seriesMeta {
		tags := m.Tags.WithoutName()
		resultSeriesMeta[i].Tags = tags
		resultSeriesMeta[i].Name =  tags.ID()
	}

	builder, err := c.controller.BlockBuilder(seriesIter.Meta(), resultSeriesMeta)
	if err != nil {
		return err
	}

	if err := builder.AddCols(bounds.Steps()); err != nil {
		return err
	}

	for seriesIter.Next() {
		values = values[0:0]
		for i, iter := range depIters {
			if !iter.Next() {
				return fmt.Errorf("incorrect number of series for block: %d", i)
			}

			s, err := iter.Current()
			if err != nil {
				return err
			}

			values = append(values, s.Values()...)
		}

		desiredLength := int(aggDuration / bounds.StepSize)
		series, err := seriesIter.Current()
		if err != nil {
			return err
		}

		for i := 0; i < series.Len(); i++ {
			val := series.ValueAtStep(i)
			values = append(values, val)
			newVal := math.NaN()
			if desiredLength <= len(values) {
				values = values[len(values)-desiredLength:]
				newVal = c.processor.Process(values)
			}

			builder.AppendValue(i, newVal)

		}
	}

	nextBlock := builder.Build()
	defer nextBlock.Close()
	return c.controller.Process(nextBlock)
}

func (c *baseNode) sweep(processedKeys map[time.Time]bool, queryStartBounds block.Bounds, queryEndBounds block.Bounds, maxBlocks int) {
	for processedTime, processed := range processedKeys {
		if !processed {
			continue
		}

		bound := block.Bounds{
			Start:    processedTime,
			Duration: queryStartBounds.Duration,
			StepSize: queryStartBounds.StepSize,
		}
		rightRangeStart := bound.Next(maxBlocks).Start
		if rightRangeStart.After(queryEndBounds.Start) {
			rightRangeStart = queryEndBounds.Start
		}

		allProcessed := true
		duration := bound.Duration
		for t := processedTime.Add(duration); !t.After(rightRangeStart); t = t.Add(duration) {
			proc, exists := processedKeys[t]
			if !proc || !exists {
				allProcessed = false
				break
			}
		}

		if allProcessed {
			c.cache.Remove(processedTime)
		}

	}
}

func lastEmpty(blks []block.Block) int {
	lastNil := len(blks) - 1
	for ; lastNil >= 0; lastNil-- {
		if blks[lastNil] == nil {
			break
		}
	}
	return lastNil
}

func firstEmpty(blks []block.Block) int {
	for firstNil := 0; firstNil < len(blks); firstNil++ {
		if blks[firstNil] == nil {
			return firstNil
		}
	}

	return len(blks)
}

// Processor is implemented by the underlying transforms
type Processor interface {
	Process(values []float64) float64
}

// MakeProcessor is a way to create a transform
type MakeProcessor func(op baseOp, controller *transform.Controller) Processor

type processRequest struct {
	blk    block.Block
	bounds block.Bounds
	deps   []block.Block
}
