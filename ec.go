package ecRedis

import (
	"fmt"
	"github.com/ScottMansfield/nanolog"
	"github.com/klauspost/reedsolomon"
	"time"
)

func NewEncoder(DataShards int, ParityShards int, ECMaxGoroutine int) reedsolomon.Encoder {
	enc, err := reedsolomon.New(DataShards, ParityShards, reedsolomon.WithMaxGoroutines(ECMaxGoroutine))
	if err != nil {
		fmt.Println("newEncoder err", err)
	}
	return enc
}

func Encoding(encoder reedsolomon.Encoder, obj []byte) ([][]byte, error) {
	// split obj first
	shards, err := encoder.Split(obj)
	if err != nil {
		fmt.Println("encoding split err", err)
		return nil, err
	}
	// Encode parity
	err = encoder.Encode(shards)
	if err != nil {
		fmt.Println("encoding encode err", err)
		return nil, err
	}
	ok, err := encoder.Verify(shards)
	if ok == false {
		panic("encoding failed")
	}
	fmt.Println("encoding verify status is ", ok)
	return shards, err
}

//func Decoding(encoder reedsolomon.Encoder, data [][]byte /*, fileSize int*/) (bytes.Buffer, error) {
func Decoding(encoder reedsolomon.Encoder, data [][]byte) error {
	//counter := 0
	//for i := range data {
	//	if data[i] == nil {
	//		counter += 1
	//	}
	//}
	//fmt.Println("Client chunkArr nil index is", counter)
	t0 := time.Now()
	ok, err := encoder.Verify(data)
	if ok {
		fmt.Println("No reconstruction needed")
	} else {
		fmt.Println("Verification failed. Reconstructing data")
		err = encoder.Reconstruct(data)
		if err != nil {
			fmt.Println("Reconstruct failed -", err)
		}
		ok, err = encoder.Verify(data)
		if !ok {
			fmt.Println("Verification failed after reconstruction, data likely corrupted.")
		}
		if err != nil {
			fmt.Println(err)
		}
	}
	time0 := time.Since(t0)
	//fmt.Println("Data status is", ok, "Decoding time is", time.Since(t))
	if err := nanolog.Log(LogDec, ok, time0.String()); err != nil {
		fmt.Println("decoding log err", err)
	}
	fmt.Println(ok)
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
