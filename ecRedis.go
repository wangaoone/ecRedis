package ecRedis

import (
	"fmt"
	"github.com/ScottMansfield/nanolog"
	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
	"github.com/google/uuid"
	"github.com/seiflotfy/cuckoofilter"
	"github.com/wangaoone/redeo/resp"
	"io"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"
)

const (
	MaxLambdaStores int = 14
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
	tmp := make([]Conn, DataShards+ParityShards)
	for i := 0; i < DataShards+ParityShards; i++ {
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
	t0 := time.Now()
	members := []consistent.Member{}
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
	time0 := time.Since(t0)
	//fmt.Println("Dial all goroutines are done!")
	if err := nanolog.Log(LogClient, "Dial", time0.String()); err != nil {
		fmt.Println(err)
	}
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

// random will generate random sequence within the lambda stores
// index and get top n id
func random(n int) []int {
	r := rand.New(rand.NewSource(time.Now().Unix()))
	return r.Perm(MaxLambdaStores)[:n]
}

func (c *Client) set(addr string, key string, val []byte, i int, lambdaId int, wg *sync.WaitGroup, reqId string) {
	//c.W[i].WriteCmdBulk("SET", key, strconv.Itoa(i), val)
	//c.Conns[addr][i].W.WriteCmdBulk("SET", key, strconv.Itoa(i), val)
	c.Conns[addr][i].W.WriteCmdClient("SET", key, strconv.Itoa(i), strconv.Itoa(lambdaId), reqId, strconv.Itoa(DataShards), strconv.Itoa(ParityShards), val) // key chunkId lambdaId reqId val
	//c.Conns[addr][i].W.WriteCmdBulkRedis("SET", key, val)
	// Flush pipeline
	//if err := c.W[i].Flush(); err != nil {
	if err := c.Conns[addr][i].W.Flush(); err != nil {
		panic(err)
	}
	wg.Done()
}

func (c *Client) EcSet(key string, val []byte) (located string, ok bool) {
	t0 := time.Now()
	var wg sync.WaitGroup

	// randomly generate destiny lambda store id
	// return top (DataShards + ParityShards) lambda index
	index := random(DataShards + ParityShards)

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

	shards, err := c.Encoding(val)
	if err != nil {
		fmt.Println("EcSet err", err)
	}
	c.ReqId = uuid.New().String()
	for i := 0; i < DataShards+ParityShards; i++ {
		//fmt.Println("shards", i, "is", shards[i])
		wg.Add(1)
		go c.set(host, key, shards[i], i, index[i], &wg, c.ReqId)
	}
	wg.Wait()
	time0 := time.Since(t0)
	//fmt.Println("EcSet all goroutines are done!")
	//if err := nanolog.Log(LogClient, "EcSet", time0.String()); err != nil {
	//	fmt.Println(err)
	//}
	c.DataEntry.SetLatency = int64(time0)
	// FIXME: dirty design which leaks abstraction to the user
	return host, true
}

func (c *Client) get(addr string, key string, wg *sync.WaitGroup, i int, reqId string) {
	tGet := time.Now()
	fmt.Println("Client send GET req timeStamp", tGet, "chunkId is", i)
	//c.W[i].WriteCmdString("GET", key)
	//c.Conns[addr][i].W.WriteCmdString("GET", key)
	c.Conns[addr][i].W.WriteCmdString("GET", key, strconv.Itoa(i), reqId, strconv.Itoa(DataShards), strconv.Itoa(ParityShards)) // cmd key chunkId reqId DataShards ParityShards
	// Flush pipeline
	//if err := c.W[i].Flush(); err != nil {
	if err := c.Conns[addr][i].W.Flush(); err != nil {
		panic(err)
	}
	wg.Done()
}

func (c *Client) EcGet(key string) (addr string, ok bool) {
	t0 := time.Now()
	var wg sync.WaitGroup

	//addr, ok := c.getHost(key)
	t := time.Now()
	member := c.Ring.LocateKey([]byte(key))
	host := member.String()
	fmt.Println("ring LocateKey costs:", time.Since(t))
	fmt.Println("GET located host: ", host)
	c.GetReqId = uuid.New().String()
	for i := 0; i < DataShards+ParityShards; i++ {
		wg.Add(1)
		go c.get(host, key, &wg, i, c.GetReqId)
	}
	wg.Wait()
	//fmt.Println("EcGet all goroutines are done!")
	time0 := time.Since(t0)
	//if err := nanolog.Log(LogClient, "EcGet", time0.String()); err != nil {
	//	fmt.Println(err)
	//}
	c.GetLatency = int64(time0)
	// FIXME: dirty design which leaks abstraction to the user
	return host, true
}

func (c *Client) rec(addr string, wg *sync.WaitGroup, i int) {
	//t0 := time.Now()
	c.ChunkArr[i] = nil
	var id int64
	// peeking response type and receive
	// chunk id
	//type0, err := c.R[i].PeekType()
	//t1 := time.Now()
	type0, err := c.Conns[addr][i].R.PeekType()
	if err != nil {
		fmt.Println("peekType err", err)
		return
	}
	//time1 := time.Since(t1)
	//fmt.Println("after 1st peektype len is", c.R[i].Buffered())
	//fmt.Println("after 1st peektype len is", c.Conns[addr][i].R.Buffered())
	//t2 := time.Now()
	switch type0 {
	case resp.TypeInt:
		id, err = c.Conns[addr][i].R.ReadInt()
		//id, err = c.Conns[addr][i].R.ReadBulkString()
		if err != nil {
			fmt.Println("typeBulkString err", err)
		}
		fmt.Println("id is ", id)
		if id == -1 {
			wg.Done()
			fmt.Println("receive enough chunks")
			return
		}
	default:
		panic("unexpected response type")
	}
	//time2 := time.Since(t2)
	// chunk
	//t3 := time.Now()
	//type1, err := c.R[i].PeekType()
	type1, err := c.Conns[addr][i].R.PeekType()
	if err != nil {
		fmt.Println("peekType err", err)
		return
	}
	//time3 := time.Since(t3)
	//t4 := time.Now()
	switch type1 {
	case resp.TypeBulk:
		//c.ChunkArr[int(id)%(redeo.DataShards+redeo.ParityShards)], err = c.R[i].ReadBulk(nil)
		c.ChunkArr[int(id)], err = c.Conns[addr][i].R.ReadBulk(nil)
		if err != nil {
			fmt.Println("typeBulk err", err)
		}
	default:
		panic("unexpected response type")
	}
	//time4 := time.Since(t4)
	//time0 := time.Since(t0)
	wg.Done()
	//fmt.Println("chunk id is", int(id)%(DataShards+ParityShards),
	//	"Client send RECEIVE req timeStamp", t0,
	//	"Client Peek ChunkId time is", time1,
	//	"Client read ChunkId time is ", time2,
	//	"Client Peek chunkBody time is", time3,
	//	"Client read chunkBody time is", time4,
	//	"RECEIVE goroutine duration time is ", time0)
	//if err := nanolog.Log(LogRec, int(id)%(DataShards+ParityShards), t0.String(), time1.String(), time2.String(),
	//	time3.String(), time4.String(), time0.String()); err != nil {
	//	fmt.Println(err)
	//}
}

func (c *Client) Receive(addr string) {
	t0 := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < DataShards+ParityShards; i++ {
		wg.Add(1)
		go c.rec(addr, &wg, i)
	}
	wg.Wait()
	time0 := time.Since(t0)
	//fmt.Println("EcReceive all goroutines are done!")
	//if err := nanolog.Log(LogClient, "EcReceive", time0.String()); err != nil {
	//	fmt.Println(err)
	//}
	c.RecLatency = int64(time0)
	if c.SetReqId != "" {
		nanolog.Log(LogClient, "set", c.SetReqId, c.SetLatency, c.RecLatency, int64(0))
	}
}

func (c *Client) Encoding(obj []byte) ([][]byte, error) {
	// split obj first
	shards, err := c.EC.Split(obj)
	if err != nil {
		fmt.Println("encoding split err", err)
		return nil, err
	}
	// Encode parity
	err = c.EC.Encode(shards)
	if err != nil {
		fmt.Println("encoding encode err", err)
		return nil, err
	}
	ok, err := c.EC.Verify(shards)
	if ok == false {
		panic("encoding failed")
	}
	fmt.Println("encoding verify status is ", ok)
	return shards, err
}

//func Decoding(encoder reedsolomon.Encoder, data [][]byte /*, fileSize int*/) (bytes.Buffer, error) {
//func Decoding(encoder reedsolomon.Encoder, data [][]byte) error {
func (c *Client) Decoding(data [][]byte) error {
	//counter := 0
	//for i := range data {
	//	if data[i] == nil {
	//		counter += 1
	//	}
	//}
	//fmt.Println("Client chunkArr nil index is", counter)
	t0 := time.Now()
	ok, err := c.EC.Verify(data)
	if ok {
		fmt.Println("No reconstruction needed")
	} else {
		fmt.Println("Verification failed. Reconstructing data")
		err = c.EC.Reconstruct(data)
		if err != nil {
			fmt.Println("Reconstruct failed -", err)
		}
		ok, err = c.EC.Verify(data)
		if !ok {
			fmt.Println("Verification failed after reconstruction, data likely corrupted.")
		}
		if err != nil {
			fmt.Println(err)
		}
	}
	time0 := time.Since(t0)
	//fmt.Println("Data status is", ok, "Decoding time is", time.Since(t))
	//if err := nanolog.Log(LogDec, ok, time0.String()); err != nil {
	//	fmt.Println("decoding log err", err)
	//}
	fmt.Println(ok)
	nanolog.Log(LogClient, "get", c.GetReqId, c.GetLatency, c.RecLatency, time0)
	return err
	// output
	//var res bytes.Buffer
	//err = encoder.Join(&res, data, fileSize)
	//if err != nil {
	//	fmt.Println(err)
	//}
	//fmt.Println("decode val len is ", len(res.Bytes()))
	//return res, err
}
