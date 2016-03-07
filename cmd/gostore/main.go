package main

import (
	"github.com/mDibyo/gostore"
)

func main() {
	k := "a"
	v := []byte{0, 1, 2, 1, 0}
	tid := gostore.NewTransaction()
	tid.Set(gostore.Key(k), gostore.Value(v))
	tid.Commit()
}
