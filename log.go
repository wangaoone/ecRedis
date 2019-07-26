package ecRedis

import (
	"github.com/ScottMansfield/nanolog"
)

var (
	LogClient nanolog.Handle
	LogRec    nanolog.Handle
	LogDec    nanolog.Handle
)

func init() {
	LogClient = nanolog.AddLogger("%s All goroutine has finished. Duration is %s")
	LogRec = nanolog.AddLogger("chunk id is %i, " +
		"Client send RECEIVE req timeStamp is %s " +
		"Client Peek ChunkId time is %s" +
		"Client read ChunkId time is %s " +
		"Client Peek chunkBody time is %s " +
		"Client read chunkBody time is %s " +
		"RECEIVE goroutine duration time is %s ")
	LogDec = nanolog.AddLogger("DataStatus is %b, Decoding time is %s")

}
