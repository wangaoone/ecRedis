package ecRedis

import (
	"bytes"
	"errors"
	"github.com/ScottMansfield/nanolog"
	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
	"github.com/google/uuid"
	"github.com/seiflotfy/cuckoofilter"
	"github.com/wangaoone/LambdaObjectstore/lib/logger"
	"github.com/wangaoone/redeo/resp"
	"io"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"
)

const (
	MaxLambdaStores int = 64
)

var (
	log = &logger.ColorLogger{
		Prefix: "EcRedis ",
		Level:  logger.LOG_LEVEL_ALL,
		Color:  true,
	}
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

func (c *Client) Dial(addrArr []string) bool {
	//t0 := time.Now()
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
		log.Debug("Dialing %s...", addr)
		if err := c.initDial(addr); err != nil {
			log.Error("Fail to dial %s: %v", addr, err)
			c.Close()
			return false
		}
	}
	//time0 := time.Since(t0)
	//fmt.Println("Dial all goroutines are done!")
	//if err := nanolog.Log(LogClient, "Dial", time0.String()); err != nil {
	//	fmt.Println(err)
	//}
	return true
}

func (c *Client) EcSet(key string, val []byte) bool {
	t0 := time.Now()
	c.Data.SetBegin = t0.UnixNano()
	var wg sync.WaitGroup

	// randomly generate destiny lambda store id
	index := random(DataShards + ParityShards)

	//addr, ok := c.getHost(key)
	//fmt.Println("in SET, key is: ", key)
	t := time.Now()
	member := c.Ring.LocateKey([]byte(key))
	host := member.String()
	log.Debug("ring LocateKey costs: %v", time.Since(t))
	log.Debug("SET located host: %s", host)

	shards, err := c.encode(val)
	if err != nil {
		log.Warn("EcSet failed to encode: %v", err)
		return false
	}
	c.Data.SetReqId = uuid.New().String()

	errs := make(chan error, DataShards+ParityShards)
	for i := 0; i < DataShards+ParityShards; i++ {
		//fmt.Println("shards", i, "is", shards[i])
		wg.Add(1)
		go c.set(host, key, shards[i], i, index[i], c.Data.SetReqId, &wg, errs)
	}
	wg.Wait()
	time0 := time.Since(t0)
	//fmt.Println("EcSet all goroutines are done!")
	//if err := nanolog.Log(LogClient, "EcSet", time0.String()); err != nil {
	//	fmt.Println(err)
	//}
	c.Data.SetLatency = int64(time0)

	select {
	case <-errs:
		close(errs)
		return false
	default:
	}

	rets := c.receive(host)
	for _, ret := range rets {
		_, err := ret.(error)
		if err {
			return false
		}
	}
	return true
}

func (c *Client) EcGet(key string, size int) (io.ReadCloser, bool) {
	t0 := time.Now()
	c.Data.GetBegin = t0.UnixNano()
	var wg sync.WaitGroup

	//addr, ok := c.getHost(key)
	//t := time.Now()
	member := c.Ring.LocateKey([]byte(key))
	host := member.String()
	//fmt.Println("ring LocateKey costs:", time.Since(t))
	//fmt.Println("GET located host: ", host)
	c.Data.GetReqId = uuid.New().String()

	errs := make(chan error, DataShards+ParityShards)
	for i := 0; i < DataShards+ParityShards; i++ {
		wg.Add(1)
		go c.get(host, key, i, c.Data.GetReqId, &wg, errs)
	}
	wg.Wait()
	//fmt.Println("EcGet all goroutines are done!")
	time0 := time.Since(t0)
	//if err := nanolog.Log(LogClient, "EcGet", time0.String()); err != nil {
	//	fmt.Println(err)
	//}
	c.Data.GetLatency = int64(time0)

	// FIXME: dirty design which leaks abstraction to the user
	select {
	case <-errs:
		close(errs)
		return nil, false
	default:
	}

	rets := c.receive(host)
	chunks := make([][]byte, len(rets))
	failed := make([]int, 0, len(rets))
	for i, ret := range rets {
		_, err := ret.(error)
		if err {
			failed = append(failed, i)
		} else {
			chunks[i] = ret.([]byte)
		}
	}

	reader, err := c.decode(chunks, size)
	if err != nil {
		return nil, false
	}

	// Try recover
	if len(failed) > 0 {
		go c.recover(host, key, c.Data.GetReqId, chunks, failed)
	}

	return reader, true
}

//func (c *Client) initDial(address string, wg *sync.WaitGroup) {
func (c *Client) initDial(address string) error {
	// initialize parallel connections under address
	tmp := make([]*Conn, DataShards+ParityShards)
	c.Conns[address] = tmp
	for i := 0; i < DataShards+ParityShards; i++ {
		cn, err := net.Dial("tcp", address)
		if err != nil {
			return err
		}
		tmp[i] = &Conn{
			conn: cn,
			W:    NewRequestWriter(cn),
			R:    NewResponseReader(cn),
		}
	}

	// initialize the cuckoo filter under address
	c.MappingTable[address] = cuckoo.NewFilter(1000000)
	return nil
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
	rand.Seed(time.Now().UnixNano())
	return rand.Perm(MaxLambdaStores)[:n]
}

func (c *Client) set(addr string, key string, val []byte, i int, lambdaId int, reqId string, wg *sync.WaitGroup, errs chan error) {
	defer wg.Done()

	//c.W[i].WriteCmdBulk("SET", key, strconv.Itoa(i), val)
	//c.Conns[addr][i].W.WriteCmdBulk("SET", key, strconv.Itoa(i), val)
	//c.Conns[addr][i].W.WriteCmdClient("SET", key, strconv.Itoa(i), strconv.Itoa(lambdaId), reqId, strconv.Itoa(DataShards), strconv.Itoa(ParityShards), val) // key chunkId lambdaId reqId val
	w := c.Conns[addr][i].W
	w.WriteMultiBulkSize(8)
	w.WriteBulkString("set")
	w.WriteBulkString(key)
	w.WriteBulkString(strconv.Itoa(i))
	w.WriteBulkString(strconv.Itoa(lambdaId))
	w.WriteBulkString(reqId)
	w.WriteBulkString(strconv.Itoa(DataShards))
	w.WriteBulkString(strconv.Itoa(ParityShards))

	//c.Conns[addr][i].W.WriteCmdBulkRedis("SET", key, val)
	// Flush pipeline
	//if err := c.W[i].Flush(); err != nil {
	if err := w.CopyBulk(bytes.NewReader(val), int64(len(val))); err != nil {
		errs <- err
		log.Warn("Failed to initiate setting %s (%s): %v", key, addr, err)
		return
	}
	if err := w.Flush(); err != nil {
		errs <- err
		log.Warn("Failed to initiate setting %s (%s): %v", key, addr, err)
		return
	}
}

func (c *Client) get(addr string, key string, i int, reqId string, wg *sync.WaitGroup, errs chan error) {
	defer wg.Done()

	//tGet := time.Now()
	//fmt.Println("Client send GET req timeStamp", tGet, "chunkId is", i)
	//c.W[i].WriteCmdString("GET", key)
	//c.Conns[addr][i].W.WriteCmdString("GET", key)
	c.Conns[addr][i].W.WriteCmdString("GET", key, strconv.Itoa(i), reqId, strconv.Itoa(DataShards), strconv.Itoa(ParityShards)) // cmd key chunkId reqId DataShards ParityShards
	// Flush pipeline
	//if err := c.W[i].Flush(); err != nil {
	if err := c.Conns[addr][i].W.Flush(); err != nil {
		errs <- err
		log.Warn("Failed to initiate getting %s (%s): %v", key, addr, err)
	}
}

func (c *Client) rec(addr string, i int, ret []interface{}, wg *sync.WaitGroup) {
	defer wg.Done()

	// peeking response type and receive
	// chunk id
	type0, err := c.Conns[addr][i].R.PeekType()
	if err != nil {
		log.Warn("PeekType error on receiving chunk %d: %v", i, err)
		ret[i] = err
		return
	}

	switch type0 {
	case resp.TypeBulk:
		chunkId, err := c.Conns[addr][i].R.ReadBulkString()
		if err != nil {
			log.Warn("Failed to read chunkId on receiving chunk %d: %v", i, err)
			ret[i] = err
			return
		}
		if chunkId == "-1" {
			log.Debug("Abandon late chunk %d", i)
			return
		}
	case resp.TypeError:
		strErr, err := c.Conns[addr][i].R.ReadError()
		if err == nil {
			err = errors.New(strErr)
		}
		log.Warn("Error on receiving chunk %d: %v", i, err)
		ret[i] = err
		return
	}

	// Read value
	valReader, err := c.Conns[addr][i].R.StreamBulk()
	if err != nil {
		log.Warn("Error on get value reader on receiving chunk %d: %v", i, err)
		ret[i] = err
		return
	}
	val, err := valReader.ReadAll()
	if err != nil {
		log.Error("Error on get value on receiving chunk %d: %v", i, err)
		ret[i] = err
		return
	}

	log.Debug("Got chunk %d", i)
	ret[i] = val

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

func (c *Client) receive(addr string) []interface{} {
	t0 := time.Now()
	var wg sync.WaitGroup
	ret := make([]interface{}, DataShards + ParityShards)
	for i := 0; i < len(ret); i++ {
		wg.Add(1)
		go c.rec(addr, i, ret, &wg)
	}
	wg.Wait()
	time0 := time.Since(t0)
	//fmt.Println("EcReceive all goroutines are done!")
	//if err := nanolog.Log(LogClient, "EcReceive", time0.String()); err != nil {
	//	fmt.Println(err)
	//}
	c.Data.RecLatency = int64(time0)
	c.Data.End = time.Now().UnixNano()
	if c.Data.SetReqId != "" {
		c.Data.Duration = c.Data.End - c.Data.SetBegin
		nanolog.Log(LogClient, "set", c.Data.SetReqId,
			c.Data.SetBegin, c.Data.Duration, c.Data.SetLatency, c.Data.RecLatency, int64(0), false, false)
	}
	return ret
}

func (c *Client) recover(addr string, key string, reqId string, shards [][]byte, failed []int) {
	var wg sync.WaitGroup
	errs := make(chan error, len(failed))
	for _, i := range failed {
		wg.Add(1)
		// lambdaId = 0, for lambdaID of a specified key is fixed on setting.
		go c.set(addr, key, shards[i], i, 0, reqId, &wg, errs)
	}
	wg.Wait()

	// FIXME: dirty design which leaks abstraction to the user
	select {
	case <-errs:
		log.Warn("Failed to recover shards of %s: %v", key, failed)
		close(errs)
	default:
		log.Info("Succeeded to recover shards of %s: %v", key, failed)
	}
}

func (c *Client) encode(obj []byte) ([][]byte, error) {
	// split obj first
	shards, err := c.EC.Split(obj)
	if err != nil {
		log.Warn("Encoding split err: %v", err)
		return nil, err
	}
	// Encode parity
	err = c.EC.Encode(shards)
	if err != nil {
		log.Warn("Encoding encode err: %v", err)
		return nil, err
	}
	ok, err := c.EC.Verify(shards)
	if ok == false {
		log.Warn("Failed to verify encoding: %v", err)
		return nil, err
	}
	log.Debug("Encoding succeeded.")
	return shards, err
}

//func decode(encoder reedsolomon.Encoder, data [][]byte /*, fileSize int*/) (bytes.Buffer, error) {
//func decode(encoder reedsolomon.Encoder, data [][]byte) error {
func (c *Client) decode(data [][]byte, size int) (io.ReadCloser, error) {
	var corruptCheck bool
	//counter := 0
	//for i := range data {
	//	if data[i] == nil {
	//		counter += 1
	//	}
	//}
	//fmt.Println("Client chunkArr nil index is", counter)
	t0 := time.Now()
	reconstructCheck, err := c.EC.Verify(data)
	if reconstructCheck {
		log.Debug("No reconstruction needed.")
	} else {
		log.Debug("Verification failed. Reconstructing data...")
		err = c.EC.Reconstruct(data)
		if err != nil {
			log.Warn("Reconstruction failed: %v", err)
			return nil, err
		}
		corruptCheck, err = c.EC.Verify(data)
		if !corruptCheck {
			log.Warn("Verification failed after reconstruction, data could be corrupted: %v", err)
			return nil, err
		}
	}
	time0 := time.Since(t0)
	//fmt.Println("Data status is", ok, "decode time is", time.Since(t))
	//if err := nanolog.Log(LogDec, ok, time0.String()); err != nil {
	//	fmt.Println("decode log err", err)
	//}
	c.Data.Duration = time.Now().UnixNano() - c.Data.GetBegin
	nanolog.Log(LogClient, "get", c.Data.GetReqId,
		c.Data.GetBegin, c.Data.Duration, c.Data.GetLatency, c.Data.RecLatency, int64(time0), reconstructCheck, corruptCheck)

	reader, writer := io.Pipe()
	go c.EC.Join(writer, data, size)
	return reader, nil
	// output
	//var res bytes.Buffer
	//err = encoder.Join(&res, data, fileSize)
	//if err != nil {
	//	fmt.Println(err)
	//}
	//fmt.Println("decode val len is ", len(res.Bytes()))
	//return res, err
}
