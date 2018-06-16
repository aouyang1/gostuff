package main

import (
	"fmt"
	"sync"
)

type Result struct {
	Data []int
	Err  error
}

type evaluator func(b []int, wg *sync.WaitGroup, bc <-chan int, fc <-chan bool, rc chan<- Result)

func evaluatorFunc(buffer []int, wg *sync.WaitGroup, beginc <-chan int, finish <-chan bool, resc chan<- Result) {
	var data []int

	for {
		select {
		case numData := <-beginc:
			if numData > len(buffer) {
				resc <- Result{data, fmt.Errorf("Asked to process more data than buffer size")}
			}

			for i := 0; i < numData; i++ {
				data = append(data, buffer[i])
			}
			fmt.Printf("Processed: %d and Got: %v\n", numData, data)

			wg.Done()

		case <-finish:
			fmt.Println("Got here")
			resc <- Result{data, nil}

		}
	}

}

func main() {

	var evalwg sync.WaitGroup
	var reswg sync.WaitGroup
	buffer := make([]int, 7)

	// Create slice of log evaluators
	numConsumers := 10
	evaluators := make([]evaluator, numConsumers)
	for i := range evaluators {
		evaluators[i] = evaluatorFunc
	}

	// Create slice of begin channels for each evaluator
	begin := make([]chan int, len(evaluators))
	for i := range begin {
		begin[i] = make(chan int)
	}

	// Create slice of finish channels for each evaluator
	finish := make([]chan bool, len(evaluators))
	for i := range finish {
		finish[i] = make(chan bool)
	}

	// Create slice of result channels
	r := make([]chan Result, len(evaluators))
	for i := range r {
		r[i] = make(chan Result)
	}

	// Receiver: Kick off evaluators
	for j, eval := range evaluators {
		go eval(buffer, &evalwg, begin[j], finish[j], r[j])
	}

	// Receiver: Kick off result reader and listen for results
	reswg.Add(len(r))
	for _, resc := range r {
		go func(resultc chan Result) {
			defer reswg.Done()
			fmt.Println(<-resultc)
		}(resc)
	}

	// Sender: Start sending data into slice of channels
	numDataPoints := 21
	for i := 0; i < numDataPoints; i++ {
		fmt.Println(buffer)
		switch {
		case i != 0 && i%len(buffer) == 0: // filled a buffer and start processing
			evalwg.Add(len(evaluators))
			for j := range evaluators {
				begin[j] <- len(buffer)
			}
			evalwg.Wait()
			buffer[i%len(buffer)] = i

		case i == numDataPoints-1: // if we're at the end then add that data and start processing
			evalwg.Add(len(evaluators))
			buffer[i%len(buffer)] = i
			for j := range evaluators {
				begin[j] <- i%len(buffer) + 1
			}
			evalwg.Wait()

		default:
			buffer[i%len(buffer)] = i // still got room, fill buffer
		}

	}
	for j := range finish {
		finish[j] <- true
	}

	// Waiting for all results to finish
	reswg.Wait()
}
