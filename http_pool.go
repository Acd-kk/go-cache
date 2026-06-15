package go_cache

import (
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
	self        string
	basePath    string
	engine      *gin.Engine
	mu          sync.RWMutex
	peers       *consistenthash.Map
	httpGetters map[string]*httpGetter
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

	p.engine.GET("/healthz", p.handleHealthz)
	p.engine.GET("/stats", p.handleStats)
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

	return nil
}

var _ PeerGetter = (*httpGetter)(nil)

func (p *HTTPPool) Set(peers ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	normalizedPeers := make([]string, 0, len(peers))
	for _, peer := range peers {
		normalizedPeer := normalizePeerURL(peer)
		normalizedPeers = append(normalizedPeers, normalizedPeer)
	}

	p.peers = consistenthash.New(defaultReplicas, nil)
	p.peers.Add(normalizedPeers...)
	p.httpGetters = make(map[string]*httpGetter, len(normalizedPeers))
	for _, peer := range normalizedPeers {
		p.httpGetters[peer] = &httpGetter{
			baseURL: peer + p.basePath,
			client:  &http.Client{Timeout: defaultHTTPTimeout},
		}
	}
}

// PickPeer picks a peer according to key
func (p *HTTPPool) PickPeer(key string) (PeerGetter, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.peers == nil {
		return nil, false
	}
	if peer := p.peers.Get(key); peer != "" && peer != p.self {
		p.Log("Pick peer %s", peer)
		return p.httpGetters[peer], true
	}
	return nil, false
}

var _ PeerPicker = (*HTTPPool)(nil)

func normalizePeerURL(peer string) string {
	if strings.HasPrefix(peer, "http://") || strings.HasPrefix(peer, "https://") {
		return peer
	}
	return "http://" + peer
}
