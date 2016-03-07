package main

import (
	"github.com/mDibyo/gostore"
	"fmt"
)

func main() {
	k := "a"
	v := []byte{0, 1, 2, 1, 0}
	tid := gostore.NewTransaction()
	if err := tid.Set(gostore.Key(k), gostore.Value(v)); err != nil {
		fmt.Println(err)
	}
	if err := tid.Commit(); err != nil {
		fmt.Println(err)
	}
}
