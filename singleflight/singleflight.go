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

// 针对相同的 key，无论 Do 被调用多少次，函数 fn 都只会被调用一次，等待 fn 调用结束了，返回返回值或错误。
func (g *Group) Do(key string, fn func() (interface{}, error)) (interface{}, error) {
	// 针对相同的key，都去用同一个请求
	if c, ok := g.m[key]; ok {
		// 如果请求进行中，等待...
		c.wg.Wait()
		// 请求结束返回结果
		return c.val, c.err
	}

	// 新的请求
	c := new(call)
	// 加锁
	c.wg.Add(1)
	// 表明key有对应的请求在处理
	g.m[key] = c

	// 针对这个请求调用一次fn
	c.val, c.err = fn()
	c.wg.Done()

	// 更新g.m
	delete(g.m, key)
	
	return c.val, c.err
}