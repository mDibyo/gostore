package gostore

import (
	"bytes"
	"testing"
)

func TestCopyByteArray(t *testing.T) {
	src := []byte("sample byte array")
	dest := CopyByteArray(src)
	if !bytes.Equal(dest, src) {
		t.Errorf("did not get back identical array.")
	}

	src[0] = ' '
	if bytes.Equal(dest, src) {
		t.Errorf("got back original array.")
	}

	if CopyByteArray(nil) != nil {
		t.Error("copy of nil was not nil")
	}
}
