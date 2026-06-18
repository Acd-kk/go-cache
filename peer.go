package go_cache

import pb "go-cache/proto"

type PeerPicker interface {
	// PickPeer 根据 key 选择应该负责的远程节点
	PickPeer(key string) (peer PeerGetter, ok bool)
}

type PeerGetter interface {
	// Get 用于从远程节点拉取缓存值
	Get(in *pb.Request, out *pb.Response) error
	// Set 用于向远程节点写入 key-value
	Set(group string, key string, value []byte) error
	// Delete 用于删除远程节点中的 key
	Delete(group string, key string) error
}
