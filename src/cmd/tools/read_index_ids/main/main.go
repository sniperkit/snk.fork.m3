/*
Sniperkit-Bot
- Date: 2018-08-11 22:33:29.968631097 +0200 CEST m=+0.112171202
- Status: analyzed
*/

package main

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/pborman/getopt"

	"github.com/sniperkit/snk.fork.m3/src/cmd/tools"
	"github.com/sniperkit/snk.fork.m3/src/dbnode/persist/fs"
	"github.com/sniperkit/snk.fork.m3x/ident"
	xlog "github.com/sniperkit/snk.fork.m3x/log"
)

func main() {
	var (
		optPathPrefix = getopt.StringLong("path-prefix", 'p', "", "Path prefix [e.g. /var/lib/m3db]")
		optNamespace  = getopt.StringLong("namespace", 'n', "", "Namespace [e.g. metrics]")
		optShard      = getopt.Uint32Long("shard", 's', 0, "Shard ID [expected format uint32]")
		optBlockstart = getopt.Int64Long("block-start", 'b', 0, "Block Start Time [in nsec]")
		log           = xlog.NewLogger(os.Stderr)
	)
	getopt.Parse()

	if *optPathPrefix == "" ||
		*optNamespace == "" ||
		*optShard < 0 ||
		*optBlockstart <= 0 {
		getopt.Usage()
		os.Exit(1)
	}

	bytesPool := tools.NewCheckedBytesPool()
	bytesPool.Init()

	fsOpts := fs.NewOptions().SetFilePathPrefix(*optPathPrefix)
	reader, err := fs.NewReader(bytesPool, fsOpts)
	if err != nil {
		log.Fatalf("could not create new reader: %v", err)
	}
	openOpts := fs.DataReaderOpenOptions{
		Identifier: fs.FileSetFileIdentifier{
			Namespace:  ident.StringID(*optNamespace),
			Shard:      *optShard,
			BlockStart: time.Unix(0, *optBlockstart),
		},
	}
	err = reader.Open(openOpts)
	if err != nil {
		log.Fatalf("unable to open reader: %v", err)
	}

	for {
		id, _, _, _, err := reader.ReadMetadata()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("err reading metadata: %v", err)
		}
		// Use fmt package so it goes to stdout instead of stderr
		fmt.Println(id.String())
	}
}
