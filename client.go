package ecRedis

import (
	"bytes"
	"github.com/google/uuid"
	"github.com/klauspost/reedsolomon"
	"github.com/wangaoone/redeo/resp"
	"net"
)

var (
	DataShards     int
	ParityShards   int
	ECMaxGoroutine int
)

type Client struct {
	id       uuid.UUID
	ConnArr  []net.Conn
	W        []*resp.RequestWriter
	R        []resp.ResponseReader
	ChunkArr [][]byte
	EC       reedsolomon.Encoder
	Rec      bytes.Buffer
}

func NewClient(d int, p int, max int) Client {
	DataShards = d
	ParityShards = p
	ECMaxGoroutine = max
	return Client{
		id:       uuid.New(),
		ConnArr:  make([]net.Conn, DataShards+ParityShards),
		W:        make([]*resp.RequestWriter, DataShards+ParityShards),
		R:        make([]resp.ResponseReader, DataShards+ParityShards),
		ChunkArr: make([][]byte, DataShards+ParityShards),
		EC:       NewEncoder(DataShards, ParityShards, ECMaxGoroutine),
	}
}
