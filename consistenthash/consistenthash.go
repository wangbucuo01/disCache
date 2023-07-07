package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
)

/*
1.问题引入：
	对于分布式缓存来说，当一个节点接收到请求，如果该节点并没有存储缓存值，那么它面临的难题是，从谁那获取数据？
	假设一共有10个节点。
	假设第一次随机选取了节点 1 ，节点 1 从数据源获取到数据的同时缓存该数据；
	那第二次，只有 1/10 的可能性再次选择节点 1, 有 9/10 的概率选择了其他节点，如果选择了其他节点，就意味着需要再一次从数据源获取数据，一般来说，这个操作是很耗时的。
	这样做，一是缓存效率低，二是各个节点上存储着相同的数据，浪费了大量的存储空间。
	那有什么办法，对于给定的 key，每一次都选择同一个节点呢？
2.解决方案：
	使用 hash 算法也能够做到这一点。把 key 的每一个字符的 ASCII 码加起来，再除以 10 取余数。
3.存在问题：
	节点数量变化了，假设移除了一台节点，那么hash%10就应该变成hash%9，所有的缓存值都失效了，节点在接收到对应的请求时，均需要重新去数据源获取数据，容易引起 缓存雪崩。
4.缓存雪崩：
	缓存在同一时刻全部失效，造成瞬时DB请求量大、压力骤增，引起雪崩。常因为缓存服务器宕机，或缓存设置了相同的过期时间引起。
5.一致性哈希
	一致性哈希算法将 key 映射到 2^32 的空间中，将这个数字首尾相连，形成一个环。
	将节点(通常使用节点的名称、编号和 IP 地址)的哈希值放置在环上。
	计算 key 的哈希值，放置在环上，顺时针寻找到离key的哈希值最近的第一个节点，就是应选取的节点。

	当某个节点变化时，只可能导致这个节点附近的key值所在的节点发生变化，而不会影响全局。
	一致性哈希算法，在新增/删除节点时，只需要重新定位该节点附近的一小部分数据，而不需要重新定位所有的节点，这就解决了上述的问题。
6.数据倾斜问题
	假设节点都映射在环的上半部分，那么下半部分的key都将会存储在上半部分的第一个节点（因为是顺时针存储），造成数据向这个节点过度倾斜，缓存节点间负载不均。
7.虚拟节点
	为了解决这个问题，引入了虚拟节点的概念，一个真实节点对应多个虚拟节点。

	假设 1 个真实节点对应 3 个虚拟节点，那么 peer1 对应的虚拟节点是 peer1-1、 peer1-2、 peer1-3（通常以添加编号的方式实现），其余节点也以相同的方式操作。
		第一步，计算虚拟节点的 Hash 值，放置在环上。
		第二步，计算 key 的 Hash 值，在环上顺时针寻找到应选取的虚拟节点，例如是 peer2-1，那么就对应真实节点 peer2。
	虚拟节点扩充了节点的数量，解决了节点较少的情况下数据容易倾斜的问题。而且代价非常小，只需要增加一个字典(map)维护真实节点与虚拟节点的映射关系即可。
*/

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
