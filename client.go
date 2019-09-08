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

func NewClient(dataShards int, parityShards int, ecMaxGoroutine int) *Client {
	DataShards = dataShards
	ParityShards = parityShards
	ECMaxGoroutine = ecMaxGoroutine
	return &Client{
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

type EcRet struct {
	DataShards      int
	ParityShards    int
	Rets            []interface{}
	Err             error
}

func NewEcRet(data,parity int) *EcRet {
	return &EcRet{
		DataShards: data,
		ParityShards: parity,
		Rets: make([]interface{}, data + parity),
	}
}

func (r *EcRet) Len() int {
	return r.DataShards + r.ParityShards
}

func (r *EcRet) Set(i int, ret interface{}) {
	r.Rets[i] = ret
}

func (r *EcRet) SetError(i int, ret interface{}) {
	r.Rets[i] = ret
	r.Err = ret.(error)
}

func (r *EcRet) Ret(i int) (ret []byte) {
	ret, _ = r.Rets[i].([]byte)
	return
}

func (r *EcRet) Error(i int) (err error) {
	err, _ = r.Rets[i].(error)
	return
}
