package go_cache

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"go-cache/consistenthash"
	pb "go-cache/proto"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"google.golang.org/protobuf/proto"
)

const (
	defaultBasePath    = "/go-cache/"
	defaultReplicas    = 50
	defaultHTTPTimeout = 2 * time.Second
)

type HTTPPool struct {
	// self 表示当前节点地址
	self     string
	basePath string
	engine   *gin.Engine
	mu       sync.RWMutex
	// peers 是一致性哈希环
	peers *consistenthash.Map
	// httpGetters 保存每个远程节点对应的访问器
	httpGetters map[string]*httpGetter
}

type cacheWriteRequest struct {
	Group string `json:"group"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

func NewHTTPPool(self string) *HTTPPool {
	p := &HTTPPool{
		self:     normalizePeerURL(self),
		basePath: defaultBasePath,
	}
	p.initRoutes()
	return p
}

func (p *HTTPPool) Log(format string, v ...interface{}) {
	log.Printf("[Server %s] %s", p.self, fmt.Sprintf(format, v...))
}

func (p *HTTPPool) initRoutes() {
	gin.SetMode(gin.ReleaseMode)
	p.engine = gin.New()
	p.engine.Use(gin.Recovery())

	// 这些接口方便查看服务状态和统计信息
	p.engine.GET("/healthz", p.handleHealthz)
	p.engine.GET("/stats", p.handleStats)
	// 这两个接口用于动态写入和删除 key
	p.engine.POST("/api/cache", p.handleSet)
	p.engine.DELETE("/api/cache/:group/*key", p.handleDelete)
	// 这是节点之间获取缓存数据的核心接口
	p.engine.GET(p.basePath+":groupname/*key", p.handleGet)
}

func (p *HTTPPool) handleGet(c *gin.Context) {
	groupName, err := url.PathUnescape(c.Param("groupname"))
	if err != nil {
		c.String(http.StatusBadRequest, "invalid group name")
		return
	}
	key, err := url.PathUnescape(c.Param("key")[1:])
	if err != nil {
		c.String(http.StatusBadRequest, "invalid key")
		return
	}

	p.Log("%s %s", c.Request.Method, c.Request.URL.Path)

	group := GetGroup(groupName)
	if group == nil {
		c.String(http.StatusNotFound, "no such group: "+groupName)
		return
	}

	// 请求进入后 最终都会走到 group 的缓存读取流程
	view, err := group.Get(key)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			c.String(http.StatusNotFound, err.Error())
			return
		}
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	body, err := proto.Marshal(&pb.Response{Value: view.ByteSlice()})
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	c.Data(http.StatusOK, "application/x-protobuf", body)
}

func (p *HTTPPool) handleHealthz(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status": "ok",
		"self":   p.self,
	})
}

func (p *HTTPPool) handleStats(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"self":   p.self,
		"groups": AllGroupStats(),
	})
}

func (p *HTTPPool) handleSet(c *gin.Context) {
	var req cacheWriteRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if req.Group == "" || req.Key == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "group and key are required"})
		return
	}

	group := GetGroup(req.Group)
	if group == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "no such group: " + req.Group})
		return
	}

	if err := group.Set(req.Key, []byte(req.Value)); err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "ok",
		"group":   req.Group,
		"key":     req.Key,
		"value":   req.Value,
	})
}

func (p *HTTPPool) handleDelete(c *gin.Context) {
	groupName, err := url.PathUnescape(c.Param("group"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid group name"})
		return
	}
	key, err := url.PathUnescape(strings.TrimPrefix(c.Param("key"), "/"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid key"})
		return
	}
	if key == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "key is required"})
		return
	}

	group := GetGroup(groupName)
	if group == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "no such group: " + groupName})
		return
	}

	if err := group.Delete(key); err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "ok",
		"group":   groupName,
		"key":     key,
	})
}

func (p *HTTPPool) Run(addr string) error {
	return p.engine.Run(addr)
}

func (p *HTTPPool) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.engine.ServeHTTP(w, r)
}

type httpGetter struct {
	baseURL string
	client  *http.Client
}

func (h *httpGetter) Get(in *pb.Request, out *pb.Response) error {
	// group 和 key 会拼成远程节点的访问路径
	u := fmt.Sprintf(
		"%v%v/%v",
		h.baseURL,
		url.PathEscape(in.GetGroup()),
		url.PathEscape(in.GetKey()),
	)
	res, err := h.client.Get(u)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned: %v", res.Status)
	}

	bytes, err := io.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("reading response body: %v", err)
	}

	if err := proto.Unmarshal(bytes, out); err != nil {
		return fmt.Errorf("decoding response body: %v", err)
	}

	// 这里完成 protobuf 二进制到结构体的反序列化
	return nil
}

func (h *httpGetter) Set(group string, key string, value []byte) error {
	body, err := json.Marshal(cacheWriteRequest{
		Group: group,
		Key:   key,
		Value: string(value),
	})
	if err != nil {
		return err
	}

	targetURL := strings.TrimSuffix(h.baseURL, defaultBasePath) + "/api/cache"
	req, err := http.NewRequest(http.MethodPost, targetURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	res, err := h.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(res.Body)
		return fmt.Errorf("server returned: %v, body=%s", res.Status, strings.TrimSpace(string(respBody)))
	}
	return nil
}

func (h *httpGetter) Delete(group string, key string) error {
	targetURL := fmt.Sprintf(
		"%s/api/cache/%s/%s",
		strings.TrimSuffix(h.baseURL, defaultBasePath),
		url.PathEscape(group),
		url.PathEscape(key),
	)
	req, err := http.NewRequest(http.MethodDelete, targetURL, nil)
	if err != nil {
		return err
	}

	res, err := h.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode == http.StatusNotFound {
		respBody, _ := io.ReadAll(res.Body)
		return fmt.Errorf("%w: %s", ErrKeyNotFound, strings.TrimSpace(string(respBody)))
	}
	if res.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(res.Body)
		return fmt.Errorf("server returned: %v, body=%s", res.Status, strings.TrimSpace(string(respBody)))
	}
	return nil
}

var _ PeerGetter = (*httpGetter)(nil)

func (p *HTTPPool) Set(peers ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// 先统一节点地址格式 避免哈希环和映射不一致
	normalizedPeers := make([]string, 0, len(peers))
	for _, peer := range peers {
		normalizedPeer := normalizePeerURL(peer)
		normalizedPeers = append(normalizedPeers, normalizedPeer)
	}

	p.peers = consistenthash.New(defaultReplicas, nil)
	p.peers.Add(normalizedPeers...)
	p.httpGetters = make(map[string]*httpGetter, len(normalizedPeers))
	for _, peer := range normalizedPeers {
		// 每个远程节点都配一个带超时的 HTTP 客户端
		p.httpGetters[peer] = &httpGetter{
			baseURL: peer + p.basePath,
			client:  &http.Client{Timeout: defaultHTTPTimeout},
		}
	}
}

// 根据key选择一个peer
func (p *HTTPPool) PickPeer(key string) (PeerGetter, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.peers == nil {
		return nil, false
	}
	// 一致性哈希会根据 key 选择目标节点
	//只有目标节点不是自己时才会返回true
	if peer := p.peers.Get(key); peer != "" && peer != p.self {
		p.Log("Pick peer %s", peer)
		return p.httpGetters[peer], true
	}
	return nil, false
}

func normalizePeerURL(peer string) string {
	if strings.HasPrefix(peer, "http://") || strings.HasPrefix(peer, "https://") {
		return peer
	}
	return "http://" + peer
}
