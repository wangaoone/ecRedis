package ecRedis

import (
	"fmt"
	"github.com/wangaoone/redeo/resp"
	"io"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"
)

const (
	DataShards      int = 2
	ParityShards    int = 1
	ECMaxGoroutine  int = 32
	MaxLambdaStores int = 14
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
	for i := 0; i < DataShards+ParityShards; i++ {
		wg.Add(1)
		go c.initialDial(address, &wg, i)
	}
	wg.Wait()
	fmt.Println("Dial all goroutines are done!")
}

func (c *Client) set(key string, val []byte, i int, lambdaId int, wg *sync.WaitGroup) {
	//
	// set will write chunk id and client uuid to proxy
	// cmd, key, client uuid, chunk id, destiny lambda store id, val
	//
	c.W[i].WriteCmdClient("SET", key, c.id.String(), strconv.Itoa(i), strconv.Itoa(lambdaId), val)
	// Flush pipeline
	if err := c.W[i].Flush(); err != nil {
		panic(err)
	}
	wg.Done()

}
func (c *Client) EcSet(key string, val []byte) {
	var wg sync.WaitGroup
	// random generate destiny lambda store id
	// return top (DataShards + ParityShards) lambda index
	index := random(DataShards + ParityShards)
	// prepare ec chunks
	shards, err := Encoding(c.EC, val)
	if err != nil {
		fmt.Println("EcSet err", err)
	}
	for i := 0; i < DataShards+ParityShards; i++ {
		wg.Add(1)
		go c.set(key, shards[i], i, index[i], &wg)
	}
	wg.Wait()
	fmt.Println("EcSet all goroutines are done!")
}

func (c *Client) get(key string, wg *sync.WaitGroup, i int) {
	c.W[i].WriteCmdGet("GET", strconv.Itoa(i), key)
	// Flush pipeline
	if err := c.W[i].Flush(); err != nil {
		panic(err)
	}
	wg.Done()
}

func (c *Client) EcGet(key string) {
	var wg sync.WaitGroup
	for i := 0; i < DataShards+ParityShards; i++ {
		wg.Add(1)
		go c.get(key, &wg, i)
	}
	wg.Wait()
	fmt.Println("EcGet all goroutines are done!")
}

func (c *Client) rec(wg *sync.WaitGroup, i int) {
	t := time.Now()
	var id int64
	// peeking response type and receive
	// client id
	type0, err := c.R[i].PeekType()
	if err != nil {
		fmt.Println("peekType err", err)
		return
	}
	fmt.Println("after 1st peektype len is", c.R[i].Buffered())

	switch type0 {
	case resp.TypeInt:
		id, err = c.R[i].ReadInt()
		if err != nil {
			fmt.Println("typeBulk err", err)
		}
		fmt.Println("id is ", id)
	default:
		panic("unexpected response type")
	}
	// chunk
	t0 := time.Now()
	type1, err := c.R[i].PeekType()
	if err != nil {
		fmt.Println("peekType err", err)
		return
	}
	fmt.Println("client peek bulk time is ", time.Since(t0))
	t1 := time.Now()
	switch type1 {
	case resp.TypeBulk:
		c.ChunkArr[int(id)%(DataShards+ParityShards)], err = c.R[i].ReadBulk(nil)
		if err != nil {
			fmt.Println("typeBulk err", err)
		}
	default:
		panic("unexpected response type")
	}
	fmt.Println("client read bulk time is ", time.Since(t1), "chunk id is", int(id)%(DataShards+ParityShards))
	wg.Done()
	fmt.Println("receive goroutine duration time is ", time.Since(t))
}

func (c *Client) Receive() {
	var wg sync.WaitGroup
	for i := 0; i < DataShards; i++ {
		wg.Add(1)
		go c.rec(&wg, i)
	}
	wg.Wait()
	fmt.Println("EcReceive all goroutines are done!")
}

// random will generate random sequence within the lambda stores index
// and get top n id
func random(n int) []int {
	r := rand.New(rand.NewSource(time.Now().Unix()))
	return r.Perm(14)[:n]
}
