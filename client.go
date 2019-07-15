package ecRedis

import (
	"bytes"
	"github.com/klauspost/reedsolomon"
	"github.com/wangaoone/redeo/resp"
	"net"
)

type Client struct {
	ConnArr []net.Conn
	W       []*resp.RequestWriter
	R       []resp.ResponseReader
	EC      reedsolomon.Encoder
	Rec     bytes.Buffer
}

func NewClient(dataShards int, parityShards int, ecMaxGoroutine int) Client {
	return Client{
		ConnArr: make([]net.Conn, dataShards+parityShards),
		W:       make([]*resp.RequestWriter, dataShards+parityShards),
		R:       make([]resp.ResponseReader, dataShards+parityShards),
		EC:      NewEncoder(dataShards, parityShards, ecMaxGoroutine),
	}
}
