package lru

import "container/list"

// list.List实现了一个双向链表
// Cache底层是一个双向链表+map
type Cache struct {
	ll *list.List
	cache map[string]*list.Element
	// 当前已使用的内存
	nbytes int64
	// 允许使用的最大内存
	maxBytes int64
	// 某条记录被移除时的回调函数:当一个entry被删除的时候函数将执行
	OnEvicted func(key string, value Value)
}


/*
	键值对 entry 是双向链表节点的数据类型，
	在链表中仍保存每个值对应的 key 的好处在于，
	淘汰队尾节点时，需要用 key 从字典中删除对应的映射。
*/
type entry struct {
	key string
	value Value
}

// Value是一个接口，允许值是一个实现了Value接口的任意类型
type Value interface {
	// 只包含一个Len（）方法，用于返回值所占用的内存大小
	Len() int
}

func New(maxBytes int64, onEvicted func(string, Value)) *Cache {
	return &Cache{
		ll: list.New(),
		cache: make(map[string]*list.Element),
		maxBytes: maxBytes,
		OnEvicted: onEvicted,
	}
}

// 查找
/*
	1. 根据key查找value
	2. 用了就说明要移到队首
*/
func (c *Cache) Get(key string) (value Value, ok bool) {
	if ele, ok := c.cache[key]; ok {
		// 1. 找这个值
		kv := ele.Value.(*entry)
		// 2. 后移
		c.ll.MoveToFront(ele)
		return kv.value, true
	}
	return
}

// 删除
// 淘汰队尾的元素
func (c *Cache) RemoveOldest() {
	ele := c.ll.Back()
	if ele != nil {
		c.ll.Remove(ele)
		kv := ele.Value.(*entry)
		delete(c.cache, kv.key)
		c.nbytes -= int64(len(kv.key)) + int64(kv.value.Len())
		if c.OnEvicted != nil {
			c.OnEvicted(kv.key, kv.value)
		}
	}
}

// 新增/修改
/*
	1.新增加到队首
	2.修改改完移到队首
*/
func (c *Cache) Add(key string, value Value) {
	if ele, ok := c.cache[key]; ok {
		c.ll.MoveToFront(ele)
		kv := ele.Value.(entry)
		c.nbytes += int64(value.Len()) - int64(kv.value.Len())
		kv.value = value
	} else {
		ele := c.ll.PushFront(&entry{key, value})
		c.cache[key] = ele
		c.nbytes += int64(len(key)) + int64(value.Len())
	}
	for c.maxBytes != 0 && c.maxBytes < c.nbytes {
		c.RemoveOldest()
	}
}

func (c *Cache) Len() int {
	return c.ll.Len()
}

