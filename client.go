package ecRedis

import (
	"bytes"
	"github.com/buraksezer/consistent"
	"github.com/klauspost/reedsolomon"
	"github.com/seiflotfy/cuckoofilter"
	"github.com/wangaoone/redeo/resp"
	"net"
)

var (
	DataShards     int
	ParityShards   int
	ECMaxGoroutine int
)

type Conn struct {
	conn net.Conn
	W    *resp.RequestWriter
	R    resp.ResponseReader
}

type Client struct {
	//ConnArr  []net.Conn
	//W        []*resp.RequestWriter
	//R        []resp.ResponseReader
	Conns        map[string][]Conn
	ChunkArr     [][]byte
	EC           reedsolomon.Encoder
	Rec          bytes.Buffer
	MappingTable map[string]*cuckoo.Filter
	Ring         *consistent.Consistent
}

func NewClient(dataShards int, parityShards int, ecMaxGoroutine int) Client {
	DataShards = dataShards
	ParityShards = parityShards
	ECMaxGoroutine = ecMaxGoroutine
	return Client{
		//ConnArr:  make([]net.Conn, dataShards+parityShards),
		//W:        make([]*resp.RequestWriter, dataShards+parityShards),
		//R:        make([]resp.ResponseReader, dataShards+parityShards),
		Conns:        make(map[string][]Conn),
		ChunkArr:     make([][]byte, DataShards+ParityShards),
		EC:           NewEncoder(DataShards, ParityShards, ECMaxGoroutine),
		MappingTable: make(map[string]*cuckoo.Filter),
	}
}
