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

func (d *DB) Index(v Value) {
	tokens := analyze(v)
	for _, t := range tokens {
		d.index[t] = append(d.index[t], v.ID)
	}
	d.data[v.ID] = v
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

func splitTextFile(filename string, numShards int) ([][]Value, error) {
	f, err := os.Open(filename) // os.OpenFile has more options if you need them
	defer f.Close()
	if err != nil {
		return [][]Value{}, err
	}

	rd := bufio.NewReader(f)
	var lines []string
	for {
		line, err := rd.ReadString('\n')

		if err == io.EOF {
			break
		}

		if err != nil {
			return [][]Value{}, err
		}
		lines = append(lines, line)
	}

	splitLines := make([][]Value, numShards)
	linesPerShard := len(lines) / numShards
	var i, j int
	for i = 0; i < numShards; i++ {
		splitLines[i] = make([]Value, linesPerShard)
		for j = 0; j < linesPerShard; j++ {
			splitLines[i][j] = Value{ID: i*linesPerShard + j, Text: lines[i*linesPerShard+j]}
		}
	}

	return splitLines, nil
}

func main() {
	numShards := 4
	lines, err := splitTextFile("alice-in-wonderland.txt", numShards)
	if err != nil {
		log.Fatal(err)
	}

	db := NewDB()
	var wg sync.WaitGroup
	wg.Add(numShards)
	for i := 0; i < numShards; i++ {
		go func(data []Value) {
			for _, d := range data {
				db.Lock()
				db.Index(d)
				db.Unlock()
			}
			wg.Done()
		}(lines[i])
	}
	wg.Wait()

	queryString := "Alice"
	res, err := db.Query(queryString)
	if err != nil {
		log.Fatal(err)
	}
	for _, r := range res {
		fmt.Printf("Doc ID: %d with Text: %s", r.ID, r.Text)
	}
	fmt.Printf("Found %d documents with query string %s\n", len(res), queryString)

}
