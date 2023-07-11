package disCache

import (
	"disCache/consistenthash"
	pb "disCache/discachepb"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/golang/protobuf/proto"
)

// HTTP服务端：提供被其他节点访问的能力(基于http)
// 为HTTPPool添加节点选择的功能
const (
	defaultBasePath = "/_discache/"
	defaultReplicas = 50
)

type HTTPPool struct {
	self     string //记录自己的地址 包括主机名IP和端口
	basePath string //节点间通讯地址的前缀
	mu       sync.Mutex
	peers    *consistenthash.Map
	// 映射远程节点与对应的 httpGetter。每一个远程节点对应一个 httpGetter，因为 httpGetter 与远程节点的地址 baseURL 有关。
	httpGetters map[string]*httpGetter
}

func NewHTTPPool(self string) *HTTPPool {
	return &HTTPPool{
		self:     self,
		basePath: defaultBasePath,
	}
}

// log
func (p *HTTPPool) Log(format string, v ...interface{}) {
	log.Printf("[Server %s] %s", p.self, fmt.Sprintf(format, v...))
}

func (p *HTTPPool) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.URL.Path, p.basePath) {
		panic("HTTPPool serving unexpected path: " + r.URL.Path)
	}

	p.Log("%s %s", r.Method, r.URL.Path)

	// 约定访问路径格式为 /<basepath>/<groupname>/<key>
	parts := strings.SplitN(r.URL.Path[len(p.basePath):], "/", 2)
	if len(parts) != 2 {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	groupName := parts[0]
	key := parts[1]
	group := GetGroup(groupName)
	if group == nil {
		http.Error(w, "no such group: "+groupName, http.StatusNotFound)
		return
	}

	view, err := group.Get(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	body, err := proto.Marshal(&pb.Response{
		Value: view.ByteSlice(),
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	//使用 w.Write() 将缓存值作为 httpResponse 的 body 返回
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(body)
}

// 实现PeerPicker接口，选择节点
func (p *HTTPPool) PickPeer(key string) (PeerGetter, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if peer := p.peers.Get(key); peer != "" && peer != p.self {
		p.Log("peer pick: %v", peer)
		return p.httpGetters[peer], true
	}
	return nil, false
}

// Set() 方法实例化了一致性哈希算法，并且添加了传入的节点 ,并为每一个节点创建了一个 HTTP 客户端 httpGetter。
func (p *HTTPPool) Set(peers ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	// 实例化一致性哈希算法
	p.peers = consistenthash.New(defaultReplicas, nil)
	// 节点上环
	p.peers.Add(peers...)
	// 为每个节点设置一个客户端对应
	p.httpGetters = make(map[string]*httpGetter, len(peers))
	for _, peer := range peers {
		p.httpGetters[peer] = &httpGetter{
			baseURL: peer + p.basePath,
		}
	}
}

// HTTP客户端：实现PeerGetter接口
type httpGetter struct {
	// 将要访问的远程节点的地址
	baseURL string
}

// 访问服务端获取值
func (h *httpGetter) Get(in *pb.Request, out *pb.Response) error {
	// 拼凑访问的服务端节点
	// h.baseURL最后默认带/
	// QueryEscape 对字符串进行转义，以便可以将其安全地放置在 URL 查询中。
	u := fmt.Sprintf("%v%v/%v", h.baseURL, url.QueryEscape(in.GetGroup()), url.QueryEscape(in.GetKey()))
	// 发起get请求
	resp, err := http.Get(u)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned : %v", resp.Status)
	}

	bytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("reading resp body err: %v", err)
	}

	if err = proto.Unmarshal(bytes, out); err != nil {
		return fmt.Errorf("decoding response body: %v", err)
	}

	return nil
}
