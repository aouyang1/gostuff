package main

import (
	"testing"
)

var data = []struct {
	doc            Document
	expectedTokens []string
}{
	{Document{ID: 0, Text: "hello my name is blargh"}, []string{"blargh", "hello", "is", "my", "name"}},
	{Document{ID: 1, Text: "hi"}, []string{"hi"}},
	{Document{ID: 2, Text: ""}, []string{}},
	{Document{ID: 3, Text: "what is going    on?"}, []string{"going", "is", "on?", "what"}},
	{Document{ID: 4, Text: "hey hey hey!!!"}, []string{"hey", "hey!!!"}},
	{Document{ID: 5, Text: "What what whaT"}, []string{"what"}},
}

func TestAnalyze(t *testing.T) {
	var result []string
	for _, d := range data {
		result = analyze(d.doc.Text)
		if len(result) != len(d.expectedTokens) {
			t.Errorf("Expected %d tokens, but got %d, res: %v", len(d.expectedTokens), len(result), result)
		} else {
			for j, v := range result {
				if v != d.expectedTokens[j] {
					t.Errorf("Expected %s, but got %s", d.expectedTokens[j], v)
				}
			}
		}
	}
}

func TestNewDB(t *testing.T) {
	db := NewDB()

	if db.index == nil {
		t.Error("Expected an initialized index map")
	}

	if db.data == nil {
		t.Error("Expected an initialized data map")
	}
}

func TestIndex(t *testing.T) {
	var found bool
	var err error

	db := NewDB()

	for _, d := range data {
		if err = db.Index(d.doc); err != nil {
			t.Errorf("Failed to index doc ID %d, %v", d.doc.ID, err)
		}

		if _, exists := db.data[d.doc.ID]; !exists {
			t.Errorf("Expected to find document id %d", d.doc.ID)
		}

		for _, expTok := range d.expectedTokens {
			if _, exists := db.index[expTok]; !exists {
				t.Errorf("Expected to find token %s in index", expTok)
			} else {
				found = false
				for _, docId := range db.index[expTok] {
					if docId == d.doc.ID {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("Did not find doc ID %d in token key %s", d.doc.ID, expTok)
				}
			}
		}
	}

	if err = db.Index(data[0].doc); err == nil {
		t.Error("Should have returned an error if indexing a document with and ID already in the db")
	}

}

func TestGet(t *testing.T) {
	db := NewDB()

	var err error
	var doc Document

	for _, d := range data {
		if err = db.Index(d.doc); err != nil {
			t.Errorf("Failed to index doc ID %d, %v", d.doc.ID, err)
		}
		doc, err = db.Get(d.doc.ID)
		if err != nil {
			t.Errorf("Failed to get doc ID %d, %v", d.doc.ID, err)
		}
		if doc.Text != d.doc.Text {
			t.Errorf("Retrieved document expected %s, but got %s", d.doc.Text, doc.Text)
		}
	}
}

func TestQuery(t *testing.T) {
	query := []struct {
		text     string
		expected []Document
	}{
		{"bLargh hello", make([]Document, 1)},
		{"asf", make([]Document, 0)},
		{"hi", make([]Document, 1)},
		{"is hi", make([]Document, 3)},
		{"hey", make([]Document, 1)},
		{"what", make([]Document, 2)},
	}

	db := NewDB()

	var res []Document
	var err error

	for i, d := range data {
		err = db.Index(d.doc)
		if err != nil {
			t.Errorf("Got an error while indexing doc ID %d, %v", d.doc.ID, err)
		}
		res, err = db.Query(query[i].text)
		if err != nil {
			t.Errorf("Got an error while querying %s, %v", query[i].text, err)
		}
		if len(res) != len(query[i].expected) {
			t.Errorf("Expected %d results, but got %d, res: %v", len(query[i].expected), len(res), res)
		}
	}
}
