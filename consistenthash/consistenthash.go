package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
)

// 一致性哈希 + 虚拟节点

type Hash func(data []byte) uint32

type Map struct {
	hash Hash
	// 虚拟节点倍数
	replicas int
	// hash环
	keys []int
	// 虚拟节点和真实节点的映射表 键是虚拟节点的哈希值，值是真实节点的名称
	hashMap map[int]string
}

func New(replicas int, fn Hash) *Map {
	m := &Map{
		replicas: replicas,
		hash:     fn,
		hashMap:  make(map[int]string),
	}
	if m.hash == nil {
		m.hash = crc32.ChecksumIEEE
	}
	return m
}

func (m *Map) Add(keys ...string) {
	for _, key := range keys {
		for i := 0; i < m.replicas; i++ {
			hash := int(m.hash([]byte(strconv.Itoa(i) + key)))
			m.keys = append(m.keys, hash)
			m.hashMap[hash] = key
		}
	}
	sort.Ints(m.keys)
}

// 选择节点
func (m *Map) Get(key string) string {
	if len(m.keys) == 0 {
		return ""
	}
	// 计算key的hash值
	hash := int(m.hash([]byte(key)))
	// 顺时针找到第一个匹配的虚拟节点的下标idx
	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})
	// 通过 hashMap 映射得到真实的节点
	return m.hashMap[m.keys[idx%len(m.keys)]]
}
