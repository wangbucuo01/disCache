package disCache

/*
 缓存值的抽象与封装
*/

// 只读数据结构：表示缓存值
type ByteView struct {
	// b 将会存储真实的缓存值。
	// 选择 byte 类型是为了能够支持任意的数据类型的存储，例如字符串、图片等。
	b []byte
}

// 实现了Len()方法，就相当于实现了lru中的Value接口，返回其所占的内存大小
func (v ByteView) Len() int {
	return len(v.b)
}

// b 是只读的，使用 ByteSlice() 方法返回一个拷贝，防止缓存值被外部程序修改。
func (v ByteView) ByteSlice() []byte {
	return cloneBytes(v.b)
}

func (v ByteView) String() string {
	return string(v.b)
}

// byte数组的拷贝方法
func cloneBytes(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)
	return c
}
