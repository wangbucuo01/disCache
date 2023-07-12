package disCache

import (
	pb "disCache/discachepb"
	"disCache/singleflight"
	"fmt"
	"log"
	"sync"
)

/*
	Group是最核心的数据结构，负责与用户的交互，并且控制缓存值存储和获取的流程
	1. 接收key -> 检查是否被缓存
			是 -> 返回缓存（1）
			否 -> 2
	2. 是否应当从远程节点获取
			是 -> 与远程节点交互，返回缓存值(2)
			否 -> 调用回调函数，获取值，返回，并添加到缓存（3）
*/

// 回调函数：缓存不存在时，需要从数据源（文件/数据库等）获取数据并加到缓存。我们设置一个统一的回调函数，不去考虑从哪个数据源获取数据，而是交给用户决定，当缓存不存在时，调用这个函数，得到源数据。
type Getter interface {
	Get(key string) ([]byte, error)
}

// 函数类型实现Getter接口，称为接口型函数，方便使用者在调用时既能够传入函数作为参数，也能够传入实现了该接口的结构体作为参数
type GetterFunc func(key string) ([]byte, error)

func (f GetterFunc) Get(key string) ([]byte, error) {
	return f(key)
}

// 核心数据结构 Group：一个缓存的命名空间
type Group struct {
	// 命名空间的唯一命名
	name string
	// 缓存未命中时获取数据源的回调（callback）
	getter Getter
	// 并发缓存
	mainCache cache
	peers     PeerPicker
	loader    *singleflight.Group
}

var (
	mu     sync.RWMutex
	groups = make(map[string]*Group)
)

func NewGroup(name string, cacheBytes int64, getter Getter) *Group {
	if getter == nil {
		panic("nil Getter")
	}
	mu.Lock()
	defer mu.Unlock()
	g := &Group{
		name:   name,
		getter: getter,
		mainCache: cache{
			cacheBytes: cacheBytes,
		},
		loader: &singleflight.Group{},
	}
	return g
}

// 获取特定名称的Group
func GetGroup(name string) *Group {
	mu.RLock()
	g := groups[name]
	mu.RUnlock()
	return g
}

// Get方法获取缓存值
func (g *Group) Get(key string) (ByteView, error) {
	if key == "" {
		return ByteView{}, fmt.Errorf("key is required")
	}

	// (1)
	if v, ok := g.mainCache.get(key); ok {
		log.Println("[disCache] hit")
		return v, nil
	}

	// (3)
	return g.load(key)
}

// 缓存不存在获取源数据
func (g *Group) load(key string) (value ByteView, err error) {
	viewi, err := g.loader.Do(key, func() (interface{}, error) {
		if g.peers != nil {
			// 选择节点
			if peer, ok := g.peers.PickPeer(key); ok {
				// 从节点中获取缓存值
				if value, err = g.getFromPeer(peer, key); err == nil {
					return value, nil
				}
				log.Println("[disCache] Failed to get from peer", err)
			}
		}
		return g.getLocally(key)
	})
	if err == nil {
		return viewi.(ByteView), nil
	}
	return
}

func (g *Group) getFromPeer(peer PeerGetter, key string) (ByteView, error) {
	req := &pb.Request{
		Group: g.name,
		Key:   key,
	}
	res := &pb.Response{}
	err := peer.Get(req, res)
	if err != nil {
		return ByteView{}, err
	}
	return ByteView{b: res.Value}, nil
}

/*
	getLocally 调用用户回调函数 g.getter.Get() 获取源数据，
	并且将源数据添加到缓存 mainCache 中（通过 populateCache 方法）
*/
func (g *Group) getLocally(key string) (ByteView, error) {
	bytes, err := g.getter.Get(key)
	if err != nil {
		return ByteView{}, err
	}
	value := ByteView{b: cloneBytes(bytes)}
	g.populateCache(key, value)
	return value, nil
}

func (g *Group) populateCache(key string, value ByteView) {
	g.mainCache.add(key, value)
}

// 注册peerPicker
func (g *Group) RegisterPeers(peers PeerPicker) {
	if g.peers != nil {
		panic("RegisterPeerPicker called more than once")
	}
	g.peers = peers
}
