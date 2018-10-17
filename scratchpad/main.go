package main

import (
	"fmt"
	"sync"
)

func hello(wg *sync.WaitGroup) {
	fmt.Println("hello")
	wg.Done()
}

func main() {
	var wg sync.WaitGroup
	wg.Add(1)
	go hello(&wg)
	wg.Wait()
	fmt.Println("blargh")
}
