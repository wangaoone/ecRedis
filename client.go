package ecRedis

import (
	"github.com/klauspost/reedsolomon"
	"github.com/wangaoone/redeo"
	"github.com/wangaoone/redeo/resp"
	"net"
)

type Client struct {
	ConnArr []net.Conn
	W       []*resp.RequestWriter
	R       []resp.ResponseReader
	EC      reedsolomon.Encoder
}

func NewClient() Client {
	return Client{ConnArr: make([]net.Conn, redeo.DataShards+redeo.ParityShards),
		W:  make([]*resp.RequestWriter, redeo.DataShards+redeo.ParityShards),
		R:  make([]resp.ResponseReader, redeo.DataShards+redeo.ParityShards),
		EC: NewEncoder(redeo.DataShards, redeo.ParityShards, redeo.ECMaxGoroutine)}
}
