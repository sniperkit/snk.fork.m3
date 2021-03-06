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

package generate

import (
	"os"
	"time"

	"github.com/sniperkit/snk.fork.m3/src/dbnode/clock"
	"github.com/sniperkit/snk.fork.m3/src/dbnode/encoding"
	"github.com/sniperkit/snk.fork.m3/src/dbnode/sharding"
	"github.com/sniperkit/snk.fork.m3/src/dbnode/ts"
	"github.com/sniperkit/snk.fork.m3x/ident"
	xtime "github.com/sniperkit/snk.fork.m3x/time"
)

// BlockConfig represents the configuration to generate a SeriesBlock
type BlockConfig struct {
	IDs       []string
	Tags      ident.Tags
	NumPoints int
	Start     time.Time
}

// Series represents a generated series of data
type Series struct {
	ID   ident.ID
	Tags ident.Tags
	Data []ts.Datapoint
}

// SeriesDataPoint represents a single data point of a generated series of data
type SeriesDataPoint struct {
	ts.Datapoint
	ID ident.ID
}

// SeriesDataPointsByTime are a sorted list of SeriesDataPoints
type SeriesDataPointsByTime []SeriesDataPoint

// SeriesBlock is a collection of Series'
type SeriesBlock []Series

// SeriesBlocksByStart is a map of time -> SeriesBlock
type SeriesBlocksByStart map[xtime.UnixNano]SeriesBlock

// Writer writes generated data to disk
type Writer interface {
	// WriteData writes the data as data files.
	WriteData(
		ns ident.ID, shards sharding.ShardSet, data SeriesBlocksByStart) error

	// WriteSnapshot writes the data as snapshot files.
	WriteSnapshot(
		ns ident.ID,
		shards sharding.ShardSet,
		data SeriesBlocksByStart,
		snapshotInterval time.Duration,
	) error

	// WriteDataWithPredicate writes all data that passes the predicate test as data files.
	WriteDataWithPredicate(
		ns ident.ID,
		shards sharding.ShardSet,
		data SeriesBlocksByStart,
		pred WriteDatapointPredicate,
	) error

	// WriteSnapshotWithPredicate writes all data that passes the predicate test as snapshot files.
	WriteSnapshotWithPredicate(
		ns ident.ID,
		shards sharding.ShardSet,
		data SeriesBlocksByStart,
		pred WriteDatapointPredicate,
		snapshotInterval time.Duration,
	) error
}

// Options represent the parameters needed for the Writer
type Options interface {
	// SetClockOptions sets the clock options
	SetClockOptions(value clock.Options) Options

	// ClockOptions returns the clock options
	ClockOptions() clock.Options

	// SetRetentionPeriod sets how long we intend to keep data in memory
	SetRetentionPeriod(value time.Duration) Options

	// RetentionPeriod returns how long we intend to keep data in memory
	RetentionPeriod() time.Duration

	// SetBlockSize sets the blockSize
	SetBlockSize(value time.Duration) Options

	// BlockSize returns the blockSize
	BlockSize() time.Duration

	// SetFilePathPrefix sets the file path prefix for sharded TSDB files
	SetFilePathPrefix(value string) Options

	// FilePathPrefix returns the file path prefix for sharded TSDB files
	FilePathPrefix() string

	// SetNewFileMode sets the new file mode
	SetNewFileMode(value os.FileMode) Options

	// NewFileMode returns the new file mode
	NewFileMode() os.FileMode

	// SetNewDirectoryMode sets the new directory mode
	SetNewDirectoryMode(value os.FileMode) Options

	// NewDirectoryMode returns the new directory mode
	NewDirectoryMode() os.FileMode

	// SetWriterBufferSize sets the buffer size for writing TSDB files
	SetWriterBufferSize(value int) Options

	// WriterBufferSize returns the buffer size for writing TSDB files
	WriterBufferSize() int

	// SetWriteEmptyShards sets whether writes are done even for empty start periods
	SetWriteEmptyShards(bool) Options

	// WriteEmptyShards returns whether writes are done even for empty start periods
	WriteEmptyShards() bool

	// SetWriteSnapshot sets whether writes are written as snapshot files
	SetWriteSnapshot(bool) Options

	// WriteSnapshots returns whether writes are written as snapshot files
	WriteSnapshot() bool

	// SetEncoderPool sets the contextPool
	SetEncoderPool(value encoding.EncoderPool) Options

	// EncoderPool returns the contextPool
	EncoderPool() encoding.EncoderPool
}

// WriteDatapointPredicate returns a boolean indicating whether a datapoint should be written.
type WriteDatapointPredicate func(dp ts.Datapoint) bool
