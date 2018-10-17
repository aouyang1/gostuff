package main

import (
	"fmt"
	"sort"
	"sync"
)

var bufferSize = 10

type LogLine struct {
	Key string
	Val int
}

type CBuff struct {
	sync.Mutex
	data    []LogLine
	mapping map[string]int
	head    int
	ptr     int
}

func NewCBuff(n int) *CBuff {
	c := &CBuff{
		data:    make([]LogLine, 0, n),
		mapping: make(map[string]int),
	}
	return c
}

func (c CBuff) Index(i int) LogLine {
	return c.data[(c.head+i)%cap(c.data)]
}

func (c CBuff) AtCapacity() bool {
	return len(c.data) == cap(c.data)
}

func (c CBuff) Get(key string) (LogLine, error) {
	if l, exists := c.mapping[key]; !exists {
		return LogLine{}, fmt.Errorf("Key %s was not found in buffer", key)
	} else {
		return c.data[l], nil
	}
}

func (c CBuff) Keys() []string {
	keys := make([]string, 0, len(c.mapping))
	for k, _ := range c.mapping {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func (c *CBuff) Update(key string, l LogLine, eval func(a, b LogLine) LogLine) {
	if idx, exists := c.mapping[key]; exists {
		c.data[idx] = eval(c.data[idx], l)
	}
}

func (c *CBuff) Add(line ...LogLine) {
	for _, l := range line {
		// update head if we're at full capacity in the buffer
		if c.ptr == c.head && cap(c.data) == len(c.data) {
			delete(c.mapping, c.Index(0).Key)
			c.head = (c.head + 1) % cap(c.data)
		}

		// add line to buffer
		if len(c.data) < cap(c.data) {
			// not at full capacity, so just append to the buffer
			c.data = append(c.data, l)
		} else {
			c.data[c.ptr] = l
		}
		// update mapping with latest log lines and overwrite previous keys
		c.mapping[l.Key] = c.ptr

		// increment pointer
		c.ptr = (c.ptr + 1) % cap(c.data)
	}
}

func (c CBuff) Flush() []LogLine {
	lines := make([]LogLine, len(c.data))
	if len(c.data) < cap(c.data) {
		lines = c.data
	} else {
		for i := 0; i < len(c.data); i++ {
			lines[i] = c.data[(c.head+i)%len(c.data)]
		}
	}
	return lines
}

func asChan(v ...LogLine) chan LogLine {
	out := make(chan LogLine)

	go func() {
		for _, val := range v {
			out <- val
		}
		close(out)
	}()

	return out
}

type state struct {
	endOfFile bool
	numToProc int
	mainBuff  []LogLine
	secBuff   *CBuff
}

func reader1(data []LogLine, s *state, startReader, finishedReader chan bool) {
	var i, j int

	for {
		select {
		case <-startReader:
			// fill up buff as much as we can
			for i = 0; i < bufferSize; i++ {
				if j < len(data) {
					s.mainBuff[i] = data[j]
				} else {
					break
				}
				j++
			}

			s.numToProc = i

			if j == len(data) {
				s.endOfFile = true
				finishedReader <- true // allows the stageSync to progress into the merge stage
				return
			}

			finishedReader <- true // allows the stageSync to progress into the merge stage
		}
	}
}

func reader2(data []LogLine, s *state, startReader, finishedReader chan bool) {
	var i, j int

	for {
		select {
		case <-startReader:
			// fill up buff as much as we can
			for i = 0; i < bufferSize; i++ {
				if j < len(data) {
					s.secBuff.Add(data[j])
				} else {
					break
				}
				j++
			}

			finishedReader <- true // allows the stageSync to progress into the merge stage
		}
	}

}

func merge(s *state, startMerge, finishMerge chan bool) {
	for {
		select {
		case <-startMerge:
			for i := 0; i < s.numToProc; i++ {
				if o, err := s.secBuff.Get(s.mainBuff[i].Key); err == nil {
					s.mainBuff[i] = LogLine{s.mainBuff[i].Key, s.mainBuff[i].Val + o.Val}
				} else {
					fmt.Printf("Could not find key %s\n", s.mainBuff[i].Key)
				}
			}

			finishMerge <- true
		}
	}
}

type Result struct {
	Data []LogLine
	Err  error
}

type evaluator func(s *state, sc, fc chan bool, rc chan Result)

func evaluatorFunc(s *state, startc, finishc chan bool, resc chan Result) {
	var data []LogLine

	for {
		select {
		case <-startc:
			if s.numToProc > len(s.mainBuff) {
				resc <- Result{data, fmt.Errorf("Asked to process more data than buffer size")}
			}

			for i := 0; i < s.numToProc; i++ {
				data = append(data, s.mainBuff[i])
			}

			if s.endOfFile {
				resc <- Result{data, nil}
				return
			}

			finishc <- true
		}
	}
}

func stageSync(finish []chan bool, start ...chan bool) {
	for {
		for _, f := range finish {
			<-f
		}
		for _, c := range start {
			c <- true
		}
	}
}

func main() {
	validKeys := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}

	expectedOut := []LogLine{}
	for i, key1 := range validKeys {
		for j, key2 := range validKeys {
			for k, key3 := range validKeys {
				expectedOut = append(expectedOut, LogLine{key1 + key2 + key3, i + j + k})
			}
		}
	}

	a := []LogLine{}
	b := []LogLine{}
	offset := -10
	for _, l := range expectedOut {
		a = append(a, LogLine{l.Key, l.Val + offset})
		b = append(b, LogLine{l.Key, -offset})
	}

	s := state{
		mainBuff: make([]LogLine, bufferSize),
		secBuff:  NewCBuff(bufferSize * 1),
	}

	// Create slice of reader start signals
	numReader := 2
	startReaders := make([]chan bool, numReader)
	finishedReader := make([]chan bool, numReader)
	for i := 0; i < numReader; i++ {
		startReaders[i] = make(chan bool)
		finishedReader[i] = make(chan bool)
	}

	// Create merge start signal
	startMerge := make(chan bool)
	finishedMerge := make(chan bool)

	// Create slice of log evaluators
	numConsumers := 1
	evaluators := make([]evaluator, numConsumers)
	for i := range evaluators {
		evaluators[i] = evaluatorFunc
	}

	startEval := make([]chan bool, len(evaluators))
	finishedEval := make([]chan bool, len(evaluators))
	for i := 0; i < len(evaluators); i++ {
		startEval[i] = make(chan bool)
		finishedEval[i] = make(chan bool)
	}

	// Create slice of result channels
	var reswg sync.WaitGroup
	r := make([]chan Result, len(evaluators))
	for i := range r {
		r[i] = make(chan Result)
	}

	go stageSync(finishedEval, startReaders...)
	go stageSync(finishedReader, startMerge)
	go stageSync([]chan bool{finishedMerge}, startEval...)

	go reader1(a, &s, startReaders[0], finishedReader[0])
	go reader2(b, &s, startReaders[1], finishedReader[1])

	go merge(&s, startMerge, finishedMerge)

	// Receiver: Kick off evaluators
	for j, eval := range evaluators {
		go eval(&s, startEval[j], finishedEval[j], r[j])
	}

	reswg.Add(len(r))

	for _, resc := range r {
		go func(resultc chan Result) {
			defer reswg.Done()
			res := <-resultc
			if res.Err != nil {
				fmt.Printf("Encountered error, %v while processing\n", res.Err)
				return
			}

			if len(res.Data) != len(expectedOut) {
				fmt.Printf("Expected %d records, but got %d\n", len(expectedOut), len(res.Data))
			}

			for i, l := range expectedOut {
				if (l.Val - res.Data[i].Val) != 0 {
					fmt.Printf("Did not add correctly on idx, %d. Got %d and expected %d\n", i, res.Data[i].Val, l.Val)
				}
			}
		}(resc)
	}

	// start reading on both readers
	for _, startc := range startReaders {
		startc <- true
	}

	reswg.Wait()
}
