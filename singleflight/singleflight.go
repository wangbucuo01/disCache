package singleflight

import "sync"

// 防止缓存击穿

// call代表正在进行中或已经结束的请求，使用锁避免重入
type call struct {
	wg sync.WaitGroup
	val interface{}
	err error
}

// 管理不同key的请求
type Group struct {
	mu sync.Mutex
	m map[string]*call
}

/*
	防止缓存击穿和穿透：
	假设对数据库的访问没有做任何限制的，很可能向数据库也发起 N 次请求，容易导致缓存击穿和穿透。
	即使对数据库做了防护，HTTP 请求是非常耗费资源的操作，针对相同的 key，发起多次请求也是没有必要的。
	那这种情况下，我们如何做到只向远端节点发起一次请求呢？
*/

func (g *Group) Do(key string, fn func() (interface{}, error)) (interface{}, error) {
	if c, ok := g.m[key]; ok {
		c.wg.Wait()
		return c.val, c.err
	}

	c := new(call)
	c.wg.Add(1)
	g.m[key] = c

	c.val, c.err = fn()
	c.wg.Done()

	delete(g.m, key)
	
	return c.val, c.err
}