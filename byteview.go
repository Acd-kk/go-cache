package go_cache

type ByteView struct {
	// b 保存真实缓存值 对外只提供只读访问
	b []byte
}

func (v ByteView) Len() int {
	return len(v.b)
}

func (v ByteView) ByteSlice() []byte {
	return cloneBytes(v.b)
}

func (v ByteView) String() string {
	return string(v.b)
}

func cloneBytes(b []byte) []byte {
	// 返回拷贝后的数据 避免外部直接修改底层切片
	c := make([]byte, len(b))
	copy(c, b)
	return c
}
