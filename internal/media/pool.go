package media

import "sync"

// FrameSize is the size in bytes of one 20ms slin16 frame: 16000 Hz * 2 bytes * 0.02s.
const FrameSize = 640

var framePool = sync.Pool{
	New: func() any {
		b := make([]byte, FrameSize)
		return &b
	},
}

// GetFrame returns a pooled FrameSize-length buffer. Callers must PutFrame it back
// once done; never grow or retain it beyond that point.
func GetFrame() *[]byte {
	buf := framePool.Get().(*[]byte)
	*buf = (*buf)[:FrameSize]
	return buf
}

// PutFrame returns a buffer obtained from GetFrame to the pool.
func PutFrame(buf *[]byte) {
	framePool.Put(buf)
}
