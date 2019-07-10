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
		fmt.Println("shards", i, "is", shards[i])
		go func() {
			c.W[i].WriteCmdBulk("SET", key, shards[i])
			c.W[i].Flush()
			fmt.Println("set and flush finish")
			//res, err := c.R[i].PeekType()
			//if err != nil {
			//	fmt.Println("typeInt err", err)
			//}
			//fmt.Println(res)
		}()

	}
}

func (c *Client) EcGet(key string, val []byte) {
	for i := 0; i < redeo.DataShards+redeo.ParityShards; i++ {
		go func() {
			// send request
			c.W[i].WriteCmdBulk("GET", key)
			// flush buffer
			c.W[i].Flush()
			fmt.Println("set and flush finish")
			// peeking response type and receive
			t, err := c.R[i].PeekType()
			if err != nil {
				fmt.Println("peekType err", err)
				return
			}
			switch t {
			case resp.TypeInt:
				res, err := c.R[i].ReadInt()
				if err != nil {
					fmt.Println("typeInt err", err)
				}
				fmt.Println(res)
			case resp.TypeBulk:
				res, err := c.R[i].ReadBulk(nil)
				if err != nil {
					fmt.Println("typeBulk err", err)
				}
				fmt.Println(res)
			default:
				panic("unexpected response type")
			}
		}()
	}
}
