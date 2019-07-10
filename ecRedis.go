package ecRedis

import (
	"fmt"
	"github.com/wangaoone/redeo"
	"github.com/wangaoone/redeo/resp"
	"io"
	"net"
)

func NewRequestWriter(wr io.Writer) *resp.RequestWriter {
	return resp.NewRequestWriter(wr)
}
func NewResponseReader(rd io.Reader) resp.ResponseReader {
	return resp.NewResponseReader(rd)
}

func dial(address string) (net.Conn, error) {
	cn, err := net.Dial("tcp", address)
	if err != nil {
		fmt.Println("dial err is ", err)
		return nil, err
	}
	return cn, err
}

func (c *Client) Dial(address string) {
	for i := 0; i < redeo.DataShards+redeo.ParityShards; i++ {
		cn, err := dial(address)
		if err != nil {
			fmt.Println("dial err is ", err)
		}
		c.ConnArr[i] = cn
		c.W[i] = NewRequestWriter(cn)
		c.R[i] = NewResponseReader(cn)
	}
}

func (c *Client) EcSet(key string, val []byte) {
	shards, err := Encoding(c.EC, val)
	if err != nil {
		fmt.Println("EcSet err", err)
	}
	for i := 0; i < redeo.DataShards+redeo.ParityShards; i++ {
		go c.W[i].WriteCmd("set", []byte(key), shards[i])
	}
}
