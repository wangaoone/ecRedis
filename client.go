package ecRedis

import (
	"github.com/buraksezer/consistent"
	"github.com/klauspost/reedsolomon"
	"github.com/seiflotfy/cuckoofilter"
	"github.com/wangaoone/redeo/resp"
	"net"
	"time"
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
	MappingTable map[string]*cuckoo.Filter
	Ring         *consistent.Consistent
	Data         DataEntry
	DataShards     int
	ParityShards   int
	Shards         int
}

func NewClient(dataShards int, parityShards int, ecMaxGoroutine int) *Client {
	return &Client{
		//ConnArr:  make([]net.Conn, dataShards+parityShards),
		//W:        make([]*resp.RequestWriter, dataShards+parityShards),
		//R:        make([]resp.ResponseReader, dataShards+parityShards),
		Conns:        make(map[string][]*Conn),
		EC:           NewEncoder(dataShards, parityShards, ecMaxGoroutine),
		MappingTable: make(map[string]*cuckoo.Filter),
		DataShards:   dataShards,
		ParityShards: parityShards,
		Shards:       dataShards + parityShards,
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

type ecRet struct {
	Shards          int
	Rets            []interface{}
	Err             error
}

func newEcRet(shards int) *ecRet {
	return &ecRet{
		Shards: shards,
		Rets: make([]interface{}, shards),
	}
}

func (r *ecRet) Len() int {
	return r.Shards
}

func (r *ecRet) Set(i int, ret interface{}) {
	r.Rets[i] = ret
}

func (r *ecRet) SetError(i int, ret interface{}) {
	r.Rets[i] = ret
	r.Err = ret.(error)
}

func (r *ecRet) Ret(i int) (ret []byte) {
	ret, _ = r.Rets[i].([]byte)
	return
}

func (r *ecRet) Error(i int) (err error) {
	err, _ = r.Rets[i].(error)
	return
}
