package main

import (
	"testing"
)

func TestCBuff(t *testing.T) {
	testData := []struct {
		lines       []LogLine
		expectedVal []int
	}{
		{[]LogLine{{"a", 1}}, []int{1}},
		{[]LogLine{{"b", 2}, {"c", 3}, {"d", 4}}, []int{1, 2, 3, 4}},
		{[]LogLine{{"e", 5}}, []int{1, 2, 3, 4, 5}},
		{[]LogLine{{"f", 6}}, []int{2, 3, 4, 5, 6}},
		{[]LogLine{{"g", 7}, {"h", 8}, {"i", 9}, {"j", 10}, {"k", 11}, {"l", 12}}, []int{8, 9, 10, 11, 12}},
	}

	testMap := []struct {
		key         string
		expectedVal int
	}{
		{"a", 1},
		{"c", 3},
		{"e", 5},
		{"a", 0},
		{"g", 0},
	}

	buff := NewCBuff(5)

	if len(buff.Flush()) != 0 {
		t.Errorf("Should have nothing in the buffer")
	}

	for idx, d := range testData {
		buff.Add(d.lines...)
		res := buff.Flush()
		if len(res) != len(d.expectedVal) {
			t.Errorf("Got %d values, but expected %d", len(d.expectedVal), len(res))
		}
		for i, l := range res {
			if l.Val != d.expectedVal[i] {
				t.Errorf("Expected %v, but got %v", l.Val, d.expectedVal[i])
			}
		}

		mapRes, _ := buff.Get(testMap[idx].key)
		if mapRes.Val != testMap[idx].expectedVal {
			t.Errorf("Expected %v, but got %v", testMap[idx].expectedVal, mapRes.Val)
		}

	}

}
