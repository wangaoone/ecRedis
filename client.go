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

type DataEntry struct {
	Cmd        string
	SetReqId   string
	GetReqId   string
	SetLatency int64
	GetLatency int64
	RecLatency int64
	SetBegin   int64
	GetBegin   int64
	End        int64
	Duration   int64
}

type Client struct {
	//ConnArr  []net.Conn
	//W        []*resp.RequestWriter
	//R        []resp.ResponseReader
	Conns        map[string][]*Conn
	ChunkArr     [][]byte
	EC           reedsolomon.Encoder
	Rec          bytes.Buffer
	MappingTable map[string]*cuckoo.Filter
	Ring         *consistent.Consistent
	Data         DataEntry
}

func NewClient(dataShards int, parityShards int, ecMaxGoroutine int) Client {
	DataShards = dataShards
	ParityShards = parityShards
	ECMaxGoroutine = ecMaxGoroutine
	return Client{
		//ConnArr:  make([]net.Conn, dataShards+parityShards),
		//W:        make([]*resp.RequestWriter, dataShards+parityShards),
		//R:        make([]resp.ResponseReader, dataShards+parityShards),
		Conns:        make(map[string][]*Conn),
		ChunkArr:     make([][]byte, DataShards+ParityShards),
		EC:           NewEncoder(DataShards, ParityShards, ECMaxGoroutine),
		MappingTable: make(map[string]*cuckoo.Filter),
	}
}

func (cli *Client) Close() {
	log.Info("Cleaning up...")
	for _, conns := range cli.Conns {
		for _, conn := range conns {
			if conn != nil {
				conn.conn.Close()
			}
		}
	}
	log.Info("Client closed.")
}
