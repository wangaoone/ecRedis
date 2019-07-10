package ecRedis

import (
	"bytes"
	"fmt"
	"github.com/klauspost/reedsolomon"
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
	fmt.Println("verify status is ", ok)
	return shards, err
}

func Decoding(encoder reedsolomon.Encoder, data [][]byte, fileSize int) (bytes.Buffer, error) {
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
		fmt.Println(ok)
	}
	// output
	var res bytes.Buffer
	err = encoder.Join(&res, data, fileSize)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(res.Bytes())
	return res, err
}
