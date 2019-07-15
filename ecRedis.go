package ecRedis

import "C"
import (
	"fmt"
	"github.com/wangaoone/redeo"
	"github.com/wangaoone/redeo/resp"
	"io"
	"net"
	"sync"
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

func (c *Client) initialDial(address string, wg *sync.WaitGroup, i int) {
	cn, err := dial(address)
	if err != nil {
		fmt.Println("dial err is ", err)
	}
	c.ConnArr[i] = cn
	c.W[i] = NewRequestWriter(cn)
	c.R[i] = NewResponseReader(cn)
	wg.Done()
}

func (c *Client) Dial(address string) {
	var wg sync.WaitGroup
	for i := 0; i < redeo.DataShards+redeo.ParityShards; i++ {
		wg.Add(1)
		go c.initialDial(address, &wg, i)
	}
	wg.Wait()
	fmt.Println("dial all goroutines are done!")
}

func (c *Client) set(key string, val []byte, wg *sync.WaitGroup, i int) {
	c.W[i].WriteCmdBulk("SET", key, val)
	// Flush pipeline
	if err := c.W[i].Flush(); err != nil {
		panic(err)
	}
	fmt.Println("SET and flush finish")
	wg.Done()

}
func (c *Client) EcSet(key string, val []byte) {
	var wg sync.WaitGroup
	shards, err := Encoding(c.EC, val)
	if err != nil {
		fmt.Println("EcSet err", err)
	}
	for i := 0; i < redeo.DataShards+redeo.ParityShards; i++ {
		fmt.Println("shards", i, "is", shards[i])
		wg.Add(1)
		go c.set(key, shards[i], &wg, i)
	}
	wg.Wait()
	fmt.Println("ecset all goroutines are done!")
}

func (c *Client) EcGet(key string) {
	//var wg sync.WaitGroup
	//wg.Add(redeo.DataShards + redeo.ParityShards)
	for i := 0; i < redeo.DataShards+redeo.ParityShards; i++ {
		//go func() {
		c.W[i].WriteCmdString("GET", key)
		// Flush pipeline
		if err := c.W[i].Flush(); err != nil {
			panic(err)
		}
		fmt.Println("GET and flush finish")
		//wg.Done()
		//res, err := c.R[i].PeekType()
		//if err != nil {
		//	fmt.Println("typeInt err", err)
		//}
		//fmt.Println(res)
		//}()
	}
	//wg.Wait()
}

func (c *Client) rec(key string, val []byte) {
	for i := 0; i < redeo.DataShards+redeo.ParityShards; i++ {
		//go func() {
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
		//}()
	}
}
