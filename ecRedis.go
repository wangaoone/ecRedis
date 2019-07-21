package ecRedis

import (
	"fmt"
	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
	"github.com/seiflotfy/cuckoofilter"
	"github.com/wangaoone/redeo"
	"github.com/wangaoone/redeo/resp"
	"io"
	//"math/rand"
	"net"
	"strconv"
	"sync"
	"time"
)

type Member string

func (m Member) String() string {
	return string(m)
}

type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	return xxhash.Sum64(data)
}

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

//func (c *Client) initDial(address string, wg *sync.WaitGroup) {
func (c *Client) initDial(address string) {
	// initialize parallel connections under address
	tmp := make([]Conn, redeo.DataShards+redeo.ParityShards)
	for i := 0; i < redeo.DataShards+redeo.ParityShards; i++ {
		cn, err := dial(address)
		if err != nil {
			fmt.Println("dial err is ", err)
		}
		tmp[i].conn = cn
		tmp[i].W = NewRequestWriter(cn)
		tmp[i].R = NewResponseReader(cn)
		c.Conns[address] = tmp
	}

	// initialize the cuckoo filter under address
	c.MappingTable[address] = cuckoo.NewFilter(1000000)
}

func (c *Client) Dial(addrArr []string) {
	members := []consistent.Member{}
	//for i := 0; i < 8; i++ {
	for _, host := range addrArr {
		member := Member(host)
		members = append(members, member)
	}
	//cfg := consistent.Config{
	//	PartitionCount:    271,
	//	ReplicationFactor: 20,
	//	Load:              1.25,
	//	Hasher:            hasher{},
	//}
	cfg := consistent.Config{
		PartitionCount:    271,
		ReplicationFactor: 20,
		Load:              1.25,
		Hasher:            hasher{},
	}
	c.Ring = consistent.New(members, cfg)
	for _, addr := range addrArr {
		fmt.Println("to dial to: ", addr)
		c.initDial(addr)
	}

	fmt.Println("Dial all goroutines are done!")
}

func (c *Client) getHost(key string) (addr string, ok bool) {
	// linear search through all filters and locate the one that holds the key
	for addr, filter := range c.MappingTable {
		found := filter.Lookup([]byte(key))
		if found { // if found, return the address
			return addr, true
		}
	}
	// otherwise, return nil
	return "", false
}

func (c *Client) set(addr string, key string, val []byte, wg *sync.WaitGroup, i int) {
	//c.W[i].WriteCmdBulk("SET", key, strconv.Itoa(i), val)
	c.Conns[addr][i].W.WriteCmdBulk("SET", key, strconv.Itoa(i), val)
	//c.Conns[addr][i].W.WriteCmdBulkRedis("SET", key, val)
	// Flush pipeline
	//if err := c.W[i].Flush(); err != nil {
	if err := c.Conns[addr][i].W.Flush(); err != nil {
		panic(err)
	}
	wg.Done()
}

func (c *Client) EcSet(key string, val []byte) (located string, ok bool) {
	var wg sync.WaitGroup

	//addr, ok := c.getHost(key)
	fmt.Println("in SET, key is: ", key)
	t := time.Now()
	member := c.Ring.LocateKey([]byte(key))
	host := member.String()
	fmt.Println("ring LocateKey costs:", time.Since(t))
	/*
		if ok == false {
			i := rand.Intn(len(c.MappingTable))
			// this means that the key does not exist in lambda store
			for a, _ := range c.MappingTable {
				if i == 0 {
					addr = a
					// don't forget to insert the non-existing key
					c.MappingTable[addr].InsertUnique([]byte(key))
					break
				}
				i--
			}
			fmt.Println("Failed to locate one host: randomly pick one: ", addr)
		}*/
	fmt.Println("SET located host: ", host)

	shards, err := Encoding(c.EC, val)
	if err != nil {
		fmt.Println("EcSet err", err)
	}

	for i := 0; i < redeo.DataShards+redeo.ParityShards; i++ {
		//fmt.Println("shards", i, "is", shards[i])
		wg.Add(1)
		go c.set(host, key, shards[i], &wg, i)
	}
	wg.Wait()
	fmt.Println("EcSet all goroutines are done!")

	// FIXME: dirty design which leaks abstraction to the user
	return host, true
}

func (c *Client) get(addr string, key string, wg *sync.WaitGroup, i int) {
	//c.W[i].WriteCmdString("GET", key)
	c.Conns[addr][i].W.WriteCmdString("GET", key)
	// Flush pipeline
	//if err := c.W[i].Flush(); err != nil {
	if err := c.Conns[addr][i].W.Flush(); err != nil {
		panic(err)
	}
	wg.Done()
}

func (c *Client) EcGet(key string) (addr string, ok bool) {
	var wg sync.WaitGroup

	//addr, ok := c.getHost(key)
	t := time.Now()
	member := c.Ring.LocateKey([]byte(key))
	host := member.String()
	fmt.Println("ring LocateKey costs:", time.Since(t))
	fmt.Println("GET located host: ", host)

	for i := 0; i < redeo.DataShards+redeo.ParityShards; i++ {
		wg.Add(1)
		go c.get(host, key, &wg, i)
	}
	wg.Wait()
	fmt.Println("EcGet all goroutines are done!")

	// FIXME: dirty design which leaks abstraction to the user
	return host, true
}

func (c *Client) rec(addr string, wg *sync.WaitGroup, i int) {
	t := time.Now()
	var id int64
	// peeking response type and receive
	// client id
	//type0, err := c.R[i].PeekType()
	type0, err := c.Conns[addr][i].R.PeekType()
	if err != nil {
		fmt.Println("peekType err", err)
		return
	}
	//fmt.Println("after 1st peektype len is", c.R[i].Buffered())
	fmt.Println("after 1st peektype len is", c.Conns[addr][i].R.Buffered())

	switch type0 {
	case resp.TypeBulk:
		id, err = c.Conns[addr][i].R.ReadInt()
		//id, err = c.Conns[addr][i].R.ReadBulkString()
		if err != nil {
			fmt.Println("typeBulkString err", err)
		}
		fmt.Println("id is ", id)
	default:
		panic("unexpected response type")
	}
	// chunk
	t0 := time.Now()
	//type1, err := c.R[i].PeekType()
	type1, err := c.Conns[addr][i].R.PeekType()
	if err != nil {
		fmt.Println("peekType err", err)
		return
	}
	fmt.Println("client peek bulk time is ", time.Since(t0))
	t1 := time.Now()
	switch type1 {
	case resp.TypeBulk:
		//c.ChunkArr[int(id)%(redeo.DataShards+redeo.ParityShards)], err = c.R[i].ReadBulk(nil)
		c.ChunkArr[int(id)%(redeo.DataShards+redeo.ParityShards)], err = c.Conns[addr][i].R.ReadBulk(nil)
		if err != nil {
			fmt.Println("typeBulk err", err)
		}
	default:
		panic("unexpected response type")
	}
	fmt.Println("client read bulk time is ", time.Since(t1), "chunk id is", int(id)%(redeo.DataShards+redeo.ParityShards))
	wg.Done()
	fmt.Println("get goroutine duration time is ", time.Since(t))
}

func (c *Client) Receive(addr string) {
	var wg sync.WaitGroup
	for i := 0; i < redeo.DataShards; i++ {
		wg.Add(1)
		go c.rec(addr, &wg, i)
	}
	wg.Wait()
	fmt.Println("EcReceive all goroutines are done!")
}
