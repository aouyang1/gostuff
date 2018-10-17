package main

import (
	"fmt"
	"sync"
)

type Blah struct {
	sync.Mutex
	Val int
}

func main() {
	fmt.Println("vim-go")

	a := Blah{}

	for i := 0; i < 5; i++ {
		go func(b *Blah, val int) {
			for {
				b.Lock()
				b.Val = val
				b.Unlock()
			}
		}(&a, i)
	}

	for {
		fmt.Printf("%+v\n", a)
	}

}
