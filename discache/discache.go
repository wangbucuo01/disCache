package disCache

import (
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

// 回调函数
type Getter interface {
	Get(key string) ([]byte, error)
}

// 函数类型实现Getter接口，称为接口型函数，
// 方便使用者在调用时既能够传入函数作为参数，也能够传入实现了该接口的结构体作为参数
type GetterFunc func(key string) ([]byte, error)

func (f GetterFunc) Get(key string) ([]byte, error) {
	return f(key)
}

// 核心数据结构 Group
// 一个缓存的命名空间
type Group struct {
	// 唯一的命名
	name string
	// 缓存未命中时获取数据源的回调（callback）
	getter Getter
	// 并发缓存
	mainCache cache
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
	}
	groups[name] = g
	return g
}
func GetGroup(name string) *Group {
	mu.RLock()
	g := groups[name]
	mu.RUnlock()
	return g
}

// Get方法
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

func (g *Group) load(key string) (value ByteView, err error) {
	return g.getLocally(key)
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
