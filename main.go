package main

import (
	discache "disCache/discache"
	"fmt"
	"log"
	"net/http"
)

// 模拟DB
var db = map[string]string{
	"Tom":  "630",
	"Jack": "589",
	"Sam":  "567",
}

func main() {
	// groupname: scores
	// 定义回调函数
	discache.NewGroup("scores", 2<<10, discache.GetterFunc(
		func(key string) ([]byte, error) {
			log.Println("[SlowDB] search key", key)
			if v, ok := db[key]; ok {
				return []byte(v), nil
			}
			return nil, fmt.Errorf("%s not exist", key)
		}))

	addr := "localhost:9999"
	peers := discache.NewHTTPPool(addr)
	log.Println("discache is running at", addr)
	log.Fatal(http.ListenAndServe(addr, peers))
}
