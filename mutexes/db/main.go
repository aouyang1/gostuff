package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
)

type Value struct {
	ID   int
	Text string
}

type DB struct {
	sync.Mutex
	index map[string][]int
	data  map[int]Value
}

func NewDB() DB {
	return DB{index: make(map[string][]int), data: make(map[int]Value)}
}

func (d *DB) Insert(values ...Value) {
	for _, v := range values {
		tokens := analyze(v)
		for _, t := range tokens {
			d.index[t] = append(d.index[t], v.ID)
		}
		d.data[v.ID] = v
	}
}

func (d DB) Query(term string) ([]Value, error) {
	var vals []Value
	var v Value
	var err error

	term = strings.ToLower(term)
	if ids, exists := d.index[term]; exists {
		for _, id := range ids {
			v, err = d.Get(id)
			if err != nil {
				return []Value{}, fmt.Errorf("query: failed to fetch all ids, %v", err)
			}
			vals = append(vals, v)
		}
	}
	return vals, nil
}

func (d DB) Get(id int) (Value, error) {
	if _, exists := d.data[id]; exists {
		return d.data[id], nil
	}
	return Value{}, fmt.Errorf("get: id %d not present", id)
}

// analyze will tokenize the text field and along with lowercasing all strings
// delimiter is a whiespace and returned list should be in sorted lexical order
// with unique words only
func analyze(v Value) []string {
	var tokens []string
	tokenMap := make(map[string]struct{})

	// tokenize by white space
	for _, s := range strings.Fields(v.Text) {

		// filter to transform text
		s = strings.ToLower(s)

		// adding to list of unique tokens
		if _, exists := tokenMap[s]; !exists {
			tokenMap[s] = struct{}{}
			tokens = append(tokens, s)
		}
	}

	sort.Strings(tokens)
	return tokens
}

func main() {
	f, err := os.Open("alice-in-wonderland.txt") // os.OpenFile has more options if you need them
	defer f.Close()
	if err != nil {
		log.Fatal(err)
	}

	rd := bufio.NewReader(f)
	var lines []string
	for {
		line, err := rd.ReadString('\n')

		if err == io.EOF {
			break
		}

		if err != nil {
			log.Fatal(err)
		}
		lines = append(lines, line)
	}

	db := NewDB()

	numProc := 4
	linesPerProc := len(lines) / numProc
	splitLines := make([][]Value, numProc)
	var i, j int
	for i = 0; i < numProc; i++ {
		splitLines[i] = make([]Value, linesPerProc)
		for j = 0; j < linesPerProc; j++ {
			splitLines[i][j] = Value{ID: i*linesPerProc + j, Text: lines[i*linesPerProc+j]}
		}
	}

	var wg sync.WaitGroup
	wg.Add(numProc)
	for i := 0; i < numProc; i++ {
		go func(data []Value) {
			for _, d := range data {
				db.Lock()
				db.Insert(d)
				db.Unlock()
			}
			wg.Done()
		}(splitLines[i])
	}
	wg.Wait()

	res, err := db.Query("Alice")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(res)

}
