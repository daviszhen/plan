// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package storage2

import (
	"context"
	"testing"
)

func TestFTSIndexBasic(t *testing.T) {
	idx := NewFTSIndex("fts_test", 0)

	docs := map[uint64]string{
		1: "the quick brown fox jumps over the lazy dog",
		2: "a quick brown cat sleeps on the mat",
		3: "the dog barked at the fox",
		4: "a lazy fox and a lazy dog",
	}

	for id, text := range docs {
		if err := idx.IndexDocument(id, text); err != nil {
			t.Fatal(err)
		}
	}

	// Search for "quick"
	ids, scores, err := idx.TextSearch(context.Background(), "quick", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 {
		t.Fatalf("expected 2 results for 'quick', got %d", len(ids))
	}
	t.Logf("'quick' results: ids=%v scores=%v", ids, scores)

	// Search for "lazy" — appears in docs 1, 4
	ids, _, err = idx.TextSearch(context.Background(), "lazy", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 {
		t.Fatalf("expected 2 results for 'lazy', got %d", len(ids))
	}

	// Search for "missing"
	ids, _, err = idx.TextSearch(context.Background(), "missing", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 0 {
		t.Fatalf("expected 0 results for 'missing', got %d", len(ids))
	}
}

func TestFTSBM25Ranking(t *testing.T) {
	idx := NewFTSIndex("fts_bm25", 0)

	// Doc 1 has "database" twice, doc 2 has it once, doc 3 doesn't have it
	idx.IndexDocument(1, "database systems database design")
	idx.IndexDocument(2, "database management introduction")
	idx.IndexDocument(3, "operating systems fundamentals")

	results, err := idx.SearchWithScores(context.Background(), "database", 10)
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	// Doc 1 should rank higher (more occurrences)
	if results[0].DocID != 1 {
		t.Fatalf("expected doc 1 to rank first, got doc %d", results[0].DocID)
	}
	if results[0].Score <= results[1].Score {
		t.Fatalf("doc 1 score (%f) should be higher than doc 2 (%f)", results[0].Score, results[1].Score)
	}
}

func TestFTSPhraseSearch(t *testing.T) {
	idx := NewFTSIndex("fts_phrase", 0)

	idx.IndexDocument(1, "the quick brown fox jumps over the lazy dog")
	idx.IndexDocument(2, "quick fox brown jumps") // same words, wrong order
	idx.IndexDocument(3, "the quick brown fox is here")

	// Phrase "quick brown fox" should match docs 1, 3 but not 2
	ids, err := idx.PhraseSearch(context.Background(), "quick brown fox", 10)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Phrase search results: %v", ids)

	hasDoc1 := false
	hasDoc3 := false
	hasDoc2 := false
	for _, id := range ids {
		switch id {
		case 1:
			hasDoc1 = true
		case 2:
			hasDoc2 = true
		case 3:
			hasDoc3 = true
		}
	}

	if !hasDoc1 {
		t.Fatal("expected doc 1 in phrase results")
	}
	if !hasDoc3 {
		t.Fatal("expected doc 3 in phrase results")
	}
	if hasDoc2 {
		t.Fatal("doc 2 should not match phrase 'quick brown fox'")
	}
}

func TestFTSBooleanSearch(t *testing.T) {
	idx := NewFTSIndex("fts_bool", 0)

	idx.IndexDocument(1, "go programming language")
	idx.IndexDocument(2, "go database design")
	idx.IndexDocument(3, "python programming language")
	idx.IndexDocument(4, "go and rust programming")

	// must=["go"], must_not=["database"]
	query := BooleanQuery{
		Must:    []string{"go"},
		MustNot: []string{"database"},
	}
	results, err := idx.BooleanSearch(context.Background(), query, 10)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Boolean search results: %v", results)

	for _, r := range results {
		if r.DocID == 2 {
			t.Fatal("doc 2 should be excluded by must_not")
		}
		if r.DocID == 3 {
			t.Fatal("doc 3 should be excluded by must (no 'go')")
		}
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results (docs 1, 4), got %d", len(results))
	}
}

func TestFTSSearchInterface(t *testing.T) {
	idx := NewFTSIndex("fts_interface", 0)
	idx.IndexDocument(1, "hello world")
	idx.IndexDocument(2, "world peace")

	ids, err := idx.Search(context.Background(), "world", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 {
		t.Fatalf("expected 2 results, got %d", len(ids))
	}

	// Non-string query should error
	_, err = idx.Search(context.Background(), 42, 10)
	if err == nil {
		t.Fatal("expected error for non-string query")
	}
}

func TestFTSMultiTermQuery(t *testing.T) {
	idx := NewFTSIndex("fts_multi", 0)

	idx.IndexDocument(1, "information retrieval systems")
	idx.IndexDocument(2, "database retrieval methods")
	idx.IndexDocument(3, "information systems design")
	idx.IndexDocument(4, "unrelated document text")

	// "information retrieval" — doc 1 has both terms (highest), 2 and 3 have one each
	results, err := idx.SearchWithScores(context.Background(), "information retrieval", 10)
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	if results[0].DocID != 1 {
		t.Fatalf("expected doc 1 first (both terms), got doc %d", results[0].DocID)
	}
}

// --- Chinese tokenizer tests ---

func TestChineseTokenizer(t *testing.T) {
	tok := NewChineseTokenizer()
	tok.AddWord("中华人民")
	tok.AddWord("人民")
	tok.AddWord("共和国")

	tokens := tok.Tokenize("中华人民共和国")

	terms := make([]string, len(tokens))
	for i, tok := range tokens {
		terms[i] = tok.Term
	}
	t.Logf("Chinese tokens: %v", terms)

	if len(tokens) != 2 || tokens[0].Term != "中华人民" || tokens[1].Term != "共和国" {
		t.Fatalf("expected [中华人民, 共和国], got %v", terms)
	}
}

func TestChineseTokenizerMixed(t *testing.T) {
	tok := NewChineseTokenizer()
	tok.AddWord("搜索")
	tok.AddWord("引擎")

	tokens := tok.Tokenize("搜索引擎test123")
	terms := make([]string, len(tokens))
	for i, tok := range tokens {
		terms[i] = tok.Term
	}
	t.Logf("Mixed tokens: %v", terms)

	if len(tokens) != 3 {
		t.Fatalf("expected 3 tokens, got %d: %v", len(tokens), terms)
	}
}

func TestFTSWithChineseTokenizer(t *testing.T) {
	idx := NewFTSIndex("fts_chinese", 0)
	tok := NewChineseTokenizer()
	tok.AddWord("数据库")
	tok.AddWord("搜索")
	tok.AddWord("引擎")
	tok.AddWord("全文")
	idx.SetTokenizer(tok)

	idx.IndexDocument(1, "数据库搜索引擎设计")
	idx.IndexDocument(2, "全文搜索引擎实现")
	idx.IndexDocument(3, "数据库管理系统")

	ids, scores, err := idx.TextSearch(context.Background(), "搜索引擎", 10)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Chinese FTS results: ids=%v scores=%v", ids, scores)
	if len(ids) != 2 {
		t.Fatalf("expected 2 results, got %d", len(ids))
	}
}

func TestSimpleTokenizer(t *testing.T) {
	tok := NewSimpleTokenizer()

	tokens := tok.Tokenize("Hello, World! This is a test-123.")
	terms := make([]string, len(tokens))
	for i, tok := range tokens {
		terms[i] = tok.Term
	}

	expected := []string{"hello", "world", "this", "is", "a", "test", "123"}
	if len(terms) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, terms)
	}
	for i := range expected {
		if terms[i] != expected[i] {
			t.Fatalf("token %d: expected %q, got %q", i, expected[i], terms[i])
		}
	}
}

func TestFTSIndexStatistics(t *testing.T) {
	idx := NewFTSIndex("fts_stats", 0)
	idx.IndexDocument(1, "hello world")
	idx.IndexDocument(2, "hello again")

	stats := idx.Statistics()
	if stats.NumEntries != 2 {
		t.Fatalf("expected 2 entries, got %d", stats.NumEntries)
	}
	if stats.IndexType != "fts" {
		t.Fatalf("expected index type 'fts', got %q", stats.IndexType)
	}
}
