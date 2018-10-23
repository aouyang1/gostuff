package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	//"sort"
	//"strings"
	//"sync"
)

type Document struct {
	ID   int
	Text string
}

type DB struct {
	index map[string][]int
	data  map[int]Document
}

// NewDB creates a DB struct and initializes the map in the index and data field
func NewDB() DB {
	//TODO: instantiate your DB struct and return it here
	return DB{}
}

// Index takes a Document and will index it into the index map and data map. The document will first be tokenized through the analyze function. For each resulting token, the doc ID will be appended to the list in the index field of the db. the key for the index field is the token string. the doc ID will be used as the key in the data field.
func (d *DB) Index(v Document) error {
	//TODO: implement your Index function here
	return nil
}

// Query will take a query string term and run it through the same analyzer as the Index function does. It will then build out a slice of documents that pertain to this particular query string. e.g. a query of "Alice Wonderland" will fetch all unique documents that contain both "alice" and "wonderland"
func (d DB) Query(term string) ([]Document, error) {
	//TODO: implement your Query function here
	return []Document{}, nil
}

// Get will retrieve the document with the specified doc ID. An error is returned if the document is not present
func (d DB) Get(id int) (Document, error) {
	//TODO: implement your Get function here
	return Document{}, nil
}

// analyze will tokenize the text string and lowercase all tokens
// delimiter is a whiespace and returned list should be in sorted lexical order
// with unique words only.
func analyze(text string) []string {
	//TODO: implement your analyzer here
	return []string{}
}

// splitTextFile reads in a text file and splits it into a slice of Document slices based on the number of shards specified in the arguments. Each line in the text file will be treated as a document.
func splitTextFile(filename string, numShards int) ([][]Document, error) {
	f, err := os.Open(filename)
	defer f.Close()
	if err != nil {
		return [][]Document{}, err
	}

	rd := bufio.NewReader(f)
	var lines []string
	for {
		line, err := rd.ReadString('\n')

		if err == io.EOF {
			break
		}

		if err != nil {
			return [][]Document{}, err
		}
		lines = append(lines, line)
	}

	splitLines := make([][]Document, numShards)
	linesPerShard := len(lines) / numShards
	var i, j int
	for i = 0; i < numShards; i++ {
		splitLines[i] = make([]Document, linesPerShard)
		for j = 0; j < linesPerShard; j++ {
			splitLines[i][j] = Document{ID: i*linesPerShard + j, Text: lines[i*linesPerShard+j]}
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
	fmt.Printf("There are %d shards of the book\n", len(lines))

	db := NewDB()
	//TODO: Create go routines to index each slice of Documents after being split from above

	// This will query for all lines that contain the word Alice or alice"
	queryString := "Alice wonderland"
	res, err := db.Query(queryString)
	if err != nil {
		log.Fatal(err)
	}
	for _, r := range res {
		fmt.Printf("Doc ID: %d with Text: %s", r.ID, r.Text)
	}
	fmt.Printf("Found %d documents with query string %s\n", len(res), queryString)

}
