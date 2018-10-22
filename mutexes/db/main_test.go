package main

import (
	"testing"
)

func TestAnalyze(t *testing.T) {
	data := []struct {
		input    Value
		expected []string
	}{
		{Value{ID: 0, Text: "hello my name is blargh"}, []string{"blargh", "hello", "is", "my", "name"}},
		{Value{ID: 1, Text: "hi"}, []string{"hi"}},
		{Value{ID: 2, Text: ""}, []string{}},
		{Value{ID: 3, Text: "what is going    on?"}, []string{"going", "is", "on?", "what"}},
		{Value{ID: 4, Text: "hey hey hey!!!"}, []string{"hey", "hey!!!"}},
		{Value{ID: 5, Text: "What what whaT"}, []string{"what"}},
	}

	var result []string
	for _, d := range data {
		result = analyze(d.input)
		if len(result) != len(d.expected) {
			t.Errorf("Expected %d tokens, but got %d", len(d.expected), len(result))
		} else {
			for j, v := range result {
				if v != d.expected[j] {
					t.Errorf("Expected %s, but got %s", d.expected[j], v)
				}
			}
		}
	}
}

func TestDB(t *testing.T) {
	data := []Value{
		{ID: 0, Text: "hello my name is blargh"},
		{ID: 1, Text: "hi"},
		{ID: 2, Text: ""},
		{ID: 3, Text: "what is going    on?"},
		{ID: 4, Text: "hey hey hey!!!"},
		{ID: 5, Text: "What what whaT"},
	}

	query := []struct {
		text     string
		expected []Value
	}{
		{"bLargh", make([]Value, 1)},
		{"asf", make([]Value, 0)},
		{"hi", make([]Value, 1)},
		{"is", make([]Value, 2)},
		{"hey", make([]Value, 1)},
		{"what", make([]Value, 2)},
	}

	db := NewDB()

	var res []Value
	var err error

	for i, d := range data {
		db.Index(d)
		res, err = db.Query(query[i].text)
		if err != nil {
			t.Errorf("Got an error while querying %s, %v", query[i].text, err)
		}
		if len(res) != len(query[i].expected) {
			t.Errorf("Expected %d results, but got %d", len(query[i].expected), len(res))
		}
	}
}
