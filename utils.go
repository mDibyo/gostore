package gostore

// CopyByteArray returns a copy of src byte array
func CopyByteArray(src []byte) []byte {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}
