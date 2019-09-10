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
	// This setting will avoid network contention.
	MaxLambdaStores int = 200
)

var (
	log = &logger.ColorLogger{
		Prefix: "EcRedis ",
		Level:  logger.LOG_LEVEL_ALL,
		Color:  true,
	}
	ErrUnexpectedResponse = errors.New("Unexpected response")
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

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

func (c *Client) EcSet(key string, val []byte, args ...interface{}) bool {
	// Debuging options
	var dryrun int
	var placements []int
	if len(args) > 0 {
		dryrun, _ = args[0].(int)
	}
	if len(args) > 1 {
		p, ok := args[1].([]int)
		if ok && len(p) >= DataShards + ParityShards {
			placements = p
		}
	}

	c.Data.Begin = time.Now()

	// randomly generate destiny lambda store id
	numClusters := MaxLambdaStores
	if dryrun > 0 {
		numClusters = dryrun
	}
	index := random(numClusters, DataShards + ParityShards)
	if dryrun > 0 && placements != nil {
		for i, ret := range index {
			placements[i] = ret
		}
		return true
	}

	//addr, ok := c.getHost(key)
	//fmt.Println("in SET, key is: ", key)
	member := c.Ring.LocateKey([]byte(key))
	host := member.String()
	// log.Debug("ring LocateKey costs: %v", time.Since(c.Data.Begin))
	// log.Debug("SET located host: %s", host)

	shards, err := c.encode(val)
	if err != nil {
		log.Warn("EcSet failed to encode: %v", err)
		return false
	}
	c.Data.ReqId = uuid.New().String()

	var wg sync.WaitGroup
	ret := NewEcRet(DataShards, ParityShards)
	for i := 0; i < ret.Len(); i++ {
		//fmt.Println("shards", i, "is", shards[i])
		wg.Add(1)
		go c.set(host, key, shards[i], i, index[i], c.Data.ReqId, &wg, ret)
	}
	wg.Wait()
	c.Data.ReqLatency = time.Since(c.Data.Begin)
	c.Data.Duration = c.Data.ReqLatency

	if ret.Err != nil {
		return false
	}

	nanolog.Log(LogClient, "set", c.Data.ReqId, c.Data.Begin.UnixNano(),
		int64(c.Data.Duration), int64(c.Data.ReqLatency), int64(0), int64(0),
		false, false)
	log.Info("Set %s %v", key, c.Data.Duration)

	if placements != nil {
		for i, ret := range ret.Rets {
			placements[i], _ = strconv.Atoi(string(ret.([]byte)))
		}
	}

	return true
}

func (c *Client) EcGet(key string, size int) (io.ReadCloser, bool) {
	c.Data.Begin = time.Now()

	//addr, ok := c.getHost(key)
	member := c.Ring.LocateKey([]byte(key))
	host := member.String()
	//fmt.Println("ring LocateKey costs:", time.Since(t))
	//fmt.Println("GET located host: ", host)
	c.Data.ReqId = uuid.New().String()

	// Send request and wait
	var wg sync.WaitGroup
	ret := NewEcRet(DataShards, ParityShards)
	for i := 0; i < ret.Len(); i++ {
		wg.Add(1)
		go c.get(host, key, i, c.Data.ReqId, &wg, ret)
	}
	wg.Wait()
	c.Data.RecLatency = time.Since(c.Data.Begin)

	// Filter results
	chunks := make([][]byte, ret.Len())
	failed := make([]int, 0, ret.Len())
	for i, _ := range ret.Rets {
		err := ret.Error(i)
		if err != nil {
			failed = append(failed, i)
		} else {
			chunks[i] = ret.Ret(i)
		}
	}

	decodeStart := time.Now()
	reader, err := c.decode(chunks, size)
	if err != nil {
		return nil, false
	}

	end := time.Now()
	c.Data.Duration = end.Sub(c.Data.Begin)
	nanolog.Log(LogClient, "get", c.Data.ReqId, c.Data.Begin.UnixNano(),
		int64(c.Data.Duration), int64(0), int64(c.Data.RecLatency), int64(end.Sub(decodeStart)),
		c.Data.AllGood, c.Data.Corrupted)
	log.Info("Got %s %v(%v %v)", key, c.Data.Duration, c.Data.RecLatency, end.Sub(decodeStart))

	// Try recover
	if len(failed) > 0 {
		c.recover(host, key, uuid.New().String(), chunks, failed)
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
func random(cluster,n int) []int {
	return rand.Perm(cluster)[:n]
}

func (c *Client) set(addr string, key string, val []byte, i int, lambdaId int, reqId string, wg *sync.WaitGroup, ret *EcRet) {
	defer wg.Done()

	w := c.Conns[addr][i].W
	w.WriteMultiBulkSize(9)
	w.WriteBulkString("set")
	w.WriteBulkString(key)
	w.WriteBulkString(strconv.Itoa(i))
	w.WriteBulkString(strconv.Itoa(lambdaId))
	w.WriteBulkString(strconv.Itoa(MaxLambdaStores))
	w.WriteBulkString(reqId)
	w.WriteBulkString(strconv.Itoa(DataShards))
	w.WriteBulkString(strconv.Itoa(ParityShards))

	// Flush pipeline
	//if err := c.W[i].Flush(); err != nil {
	if err := w.CopyBulk(bytes.NewReader(val), int64(len(val))); err != nil {
		ret.SetError(i, err)
		log.Warn("Failed to initiate setting %d@%s(%s): %v", i, key, addr, err)
		return
	}
	if err := w.Flush(); err != nil {
		ret.SetError(i, err)
		log.Warn("Failed to initiate setting %d@%s(%s): %v", i, key, addr, err)
		return
	}

	log.Debug("Initiated setting %d@%s(%s)", i, key, addr)
	c.rec("Set", addr, i, reqId, ret, nil)
}

func (c *Client) get(addr string, key string, i int, reqId string, wg *sync.WaitGroup, ret *EcRet) {
	defer wg.Done()

	//tGet := time.Now()
	//fmt.Println("Client send GET req timeStamp", tGet, "chunkId is", i)
	c.Conns[addr][i].W.WriteCmdString(
		"get", key, strconv.Itoa(i),
		reqId, strconv.Itoa(DataShards), strconv.Itoa(ParityShards)) // cmd key chunkId reqId DataShards ParityShards

	// Flush pipeline
	//if err := c.W[i].Flush(); err != nil {
	if err := c.Conns[addr][i].W.Flush(); err != nil {
		ret.SetError(i, err)
		log.Warn("Failed to initiate getting %d@%s(%s): %v", i, key, addr, err)
	}

	log.Debug("Initiated getting %d@%s(%s)", i, key, addr)
	c.rec("Got", addr, i, reqId, ret, nil)
}

func (c *Client) rec(prompt string, addr string, i int, reqId string,ret *EcRet, wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}

	// peeking response type and receive
	// chunk id
	type0, err := c.Conns[addr][i].R.PeekType()
	if err != nil {
		log.Warn("PeekType error on receiving chunk %d: %v", i, err)
		ret.SetError(i, err)
		return
	}

	switch type0 {
	case resp.TypeError:
		strErr, err := c.Conns[addr][i].R.ReadError()
		if err == nil {
			err = errors.New(strErr)
		}
		log.Warn("Error on receiving chunk %d: %v", i, err)
		ret.SetError(i, err)
		return
	}

	respId, err := c.Conns[addr][i].R.ReadBulkString()
	if err != nil {
		log.Warn("Failed to read reqId on receiving chunk %d: %v", i, err)
		ret.SetError(i, err)
		return
	}
	if respId != reqId {
		log.Warn("Unexpected response %s, want %s, chunk %d", respId, reqId, i)
		// Skip fields
		_, _ = c.Conns[addr][i].R.ReadBulkString()
		_ = c.Conns[addr][i].R.SkipBulk()
		ret.SetError(i, ErrUnexpectedResponse)
		return
	}

	chunkId, err := c.Conns[addr][i].R.ReadBulkString()
	if err != nil {
		log.Warn("Failed to read chunkId on receiving chunk %d: %v", i, err)
		ret.SetError(i, err)
		return
	}
	if chunkId == "-1" {
		log.Debug("Abandon late chunk %d", i)
		return
	}

	// Read value
	valReader, err := c.Conns[addr][i].R.StreamBulk()
	if err != nil {
		log.Warn("Error on get value reader on receiving chunk %d: %v", i, err)
		ret.SetError(i, err)
		return
	}
	val, err := valReader.ReadAll()
	if err != nil {
		log.Error("Error on get value on receiving chunk %d: %v", i, err)
		ret.SetError(i, err)
		return
	}

	log.Debug("%s chunk %d", prompt, i)
	ret.Set(i, val)
}

func (c *Client) recover(addr string, key string, reqId string, shards [][]byte, failed []int) {
	var wg sync.WaitGroup
	ret := NewEcRet(DataShards, ParityShards)
	for _, i := range failed {
		wg.Add(1)
		// lambdaId = 0, for lambdaID of a specified key is fixed on setting.
		go c.set(addr, key, shards[i], i, 0, reqId, &wg, ret)
	}
	wg.Wait()

	if ret.Err != nil {
		log.Warn("Failed to recover shards of %s: %v", key, failed)
	} else {
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

func (c *Client) decode(data [][]byte, size int) (io.ReadCloser, error) {
	c.Data.AllGood, _ = c.EC.Verify(data)
	if c.Data.AllGood {
		log.Debug("No reconstruction needed.")
	} else {
		log.Debug("Verification failed. Reconstructing data...")
		err := c.EC.Reconstruct(data)
		if err != nil {
			log.Warn("Reconstruction failed: %v", err)
			return nil, err
		}
		c.Data.Corrupted, err = c.EC.Verify(data)
		if !c.Data.Corrupted {
			log.Warn("Verification failed after reconstruction, data could be corrupted: %v", err)
			return nil, err
		} else {
			log.Debug("Reconstructed")
		}
	}

	reader, writer := io.Pipe()
	go c.EC.Join(writer, data, size)
	return reader, nil
}
