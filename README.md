# ecRedis

ecRedis is the client library for [InfiniCache](https://github.com/mason-leap-lab/InfiniCache)


## Examples
A simple client example with PUT/GET:
```go

package main

import (
	"github.com/wangaoone/ecRedis"
	"math/rand"
	"strings"
)

var addrList = "127.0.0.1:6378"

func main() {
	// initial object with random value
	var val []byte
	val = make([]byte, 1024)
	rand.Read(val)

	// parse server address
	addrArr := strings.Split(addrList, ",")

	// initial new ecRedis client
	client := ecRedis.NewClient(10, 2, 32)

	// start dial and PUT/GET
	client.Dial(addrArr)
	client.EcSet("foo", val)
	client.EcGet("foo", 1024)
}
```