package ecRedis

import (
	"bytes"
	"github.com/buraksezer/consistent"
	"github.com/klauspost/reedsolomon"
	"github.com/seiflotfy/cuckoofilter"
	"github.com/wangaoone/redeo/resp"
	"net"
	"time"
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
	ReqId      string
	Begin      time.Time
	ReqLatency time.Duration
	RecLatency time.Duration
	Duration   time.Duration
	AllGood    bool
	Corrupted  bool
}

type Client struct {
	//ConnArr  []net.Conn
	//W        []*resp.RequestWriter
	//R        []resp.ResponseReader
	Conns        map[string][]*Conn
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
