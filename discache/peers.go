package disCache

import pb "disCache/discachepb"

// 分布式节点 通信 获取缓存值
/*
使用一致性哈希选择节点
	1.判断是否是远程节点
		（1）是：HTTP客户端访问远程节点
			a.成功： 服务端返回返回值
			b.失败：回退到本地节点处理
		（2）否：回退到本地节点处理
*/

// 选择节点
type PeerPicker interface {
	PickPeer(key string) (peer PeerGetter, ok bool)
}

// 获取对应节点的缓存值
type PeerGetter interface {
	// Get(group string, key string) ([]byte, error)
	Get(in *pb.Request, out *pb.Response) error
}

