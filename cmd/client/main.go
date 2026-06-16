package main

import (
	"flag"
	"fmt"
	pb "go-cache/proto"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
)

const basePath = "/go-cache/"

func main() {
	server := flag.String("server", "localhost:8001", "target cache server address")
	group := flag.String("group", "scores", "cache group name")
	key := flag.String("key", "kk", "cache key")
	timeout := flag.Duration("timeout", 2*time.Second, "request timeout")
	flag.Parse()

	if *key == "" {
		log.Fatal("key is required")
	}

	u := fmt.Sprintf(
		"%s%s%s/%s",
		normalizeServer(*server),
		basePath,
		url.PathEscape(*group),
		url.PathEscape(*key),
	)

	client := &http.Client{Timeout: *timeout}
	// 客户端直接请求某个缓存节点
	res, err := client.Get(u)
	if err != nil {
		log.Fatal(err)
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		log.Fatal(err)
	}

	if res.StatusCode != http.StatusOK {
		log.Fatalf("request failed: %s, body=%s", res.Status, strings.TrimSpace(string(body)))
	}

	out := &pb.Response{}
	if err := proto.Unmarshal(body, out); err != nil {
		log.Fatal(err)
	}

	// 服务端返回的是 protobuf 二进制 这里再解码成真实值
	fmt.Println(string(out.GetValue()))
}

func normalizeServer(server string) string {
	if strings.HasPrefix(server, "http://") || strings.HasPrefix(server, "https://") {
		return server
	}
	return "http://" + server
}
