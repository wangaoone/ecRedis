package ecRedis

import (
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
