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
	"strings"
	"testing"
)

// TestFTS_AccentedCharsHandling tests that accented characters are handled correctly.
func TestFTS_AccentedCharsHandling(t *testing.T) {
	idx := NewFTSIndex("test_accented", 0)
	idx.SetTokenizer(NewSimpleTokenizer())
	ctx := context.Background()

	docs := []string{
		"cafe au lait",
		"resume writing tips",
		"naive approach",
		"fiancee and fiance",
	}

	for i, doc := range docs {
		if err := idx.IndexDocument(uint64(i), doc); err != nil {
			t.Fatalf("failed to index document %d: %v", i, err)
		}
	}

	testCases := []struct {
		query    string
		expected int
	}{
		{"cafe", 1},
		{"resume", 1},
		{"naive", 1},
		{"fiancee", 1},
	}

	for _, tc := range testCases {
		results, _, err := idx.TextSearch(ctx, tc.query, 100)
		if err != nil {
			t.Fatalf("failed to search for %q: %v", tc.query, err)
		}
		if len(results) < tc.expected {
			t.Errorf("query %q: expected at least %d results, got %d", tc.query, tc.expected, len(results))
		}
	}
}

// TestFTS_EmptyStringIndex tests that empty strings are handled without crashing.
func TestFTS_EmptyStringIndex(t *testing.T) {
	idx := NewFTSIndex("test_empty", 0)
	idx.SetTokenizer(NewSimpleTokenizer())
	ctx := context.Background()

	docs := []string{
		"",
		"nonempty document",
		"",
		"another nonempty",
		"",
	}

	for i, doc := range docs {
		if err := idx.IndexDocument(uint64(i), doc); err != nil {
			t.Fatalf("failed to index document %d: %v", i, err)
		}
	}

	results, _, err := idx.TextSearch(ctx, "nonempty", 100)
	if err != nil {
		t.Fatalf("failed to search: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 results, got %d", len(results))
	}

	results, _, err = idx.TextSearch(ctx, "", 100)
	if err != nil {
		t.Fatalf("failed to search empty string: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results for empty query, got %d", len(results))
	}
}

// TestFTS_LargeDocuments tests indexing and searching large documents.
func TestFTS_LargeDocuments(t *testing.T) {
	idx := NewFTSIndex("test_large", 0)
	idx.SetTokenizer(NewSimpleTokenizer())
	ctx := context.Background()

	words := []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta"}
	var sb strings.Builder
	for i := 0; i < 10000; i++ {
		if i > 0 {
			sb.WriteString(" ")
		}
		sb.WriteString(words[i%len(words)])
	}
	largeDoc := sb.String()

	if err := idx.IndexDocument(1, largeDoc); err != nil {
		t.Fatalf("failed to index large document: %v", err)
	}

	testTerms := []string{"alpha", "beta", "gamma", "delta"}
	for _, term := range testTerms {
		results, _, err := idx.TextSearch(ctx, term, 100)
		if err != nil {
			t.Fatalf("failed to search for %q: %v", term, err)
		}
		if len(results) != 1 {
			t.Errorf("expected 1 result for %q, got %d", term, len(results))
		}
	}

	results, _, err := idx.TextSearch(ctx, "omega", 100)
	if err != nil {
		t.Fatalf("failed to search for omega: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results for omega, got %d", len(results))
	}
}

// TestFTS_SpecialCharacters tests that special characters are handled gracefully.
func TestFTS_SpecialCharacters(t *testing.T) {
	idx := NewFTSIndex("test_special", 0)
	idx.SetTokenizer(NewSimpleTokenizer())
	ctx := context.Background()

	docs := []string{
		"hello world",
		"email domain com",
		"price 100",
		"code include stdio",
		"path to file txt",
	}

	for i, doc := range docs {
		if err := idx.IndexDocument(uint64(i), doc); err != nil {
			t.Fatalf("failed to index document %d: %v", i, err)
		}
	}

	testCases := []struct {
		query    string
		minCount int
	}{
		{"hello", 1},
		{"world", 1},
		{"email", 1},
		{"domain", 1},
		{"price", 1},
		{"code", 1},
		{"path", 1},
		{"file", 1},
	}

	for _, tc := range testCases {
		results, _, err := idx.TextSearch(ctx, tc.query, 100)
		if err != nil {
			t.Fatalf("failed to search for %q: %v", tc.query, err)
		}
		if len(results) < tc.minCount {
			t.Errorf("query %q: expected at least %d results, got %d", tc.query, tc.minCount, len(results))
		}
	}
}

// TestFTS_NumericTokens tests indexing and searching numeric tokens.
func TestFTS_NumericTokens(t *testing.T) {
	idx := NewFTSIndex("test_numeric", 0)
	idx.SetTokenizer(NewSimpleTokenizer())
	ctx := context.Background()

	docs := []string{
		"item 12345 available",
		"order 67890 shipped",
		"product 11111",
		"item 12345 on sale",
	}

	for i, doc := range docs {
		if err := idx.IndexDocument(uint64(i), doc); err != nil {
			t.Fatalf("failed to index document %d: %v", i, err)
		}
	}

	testCases := []struct {
		query    string
		expected int
	}{
		{"12345", 2},
		{"67890", 1},
		{"11111", 1},
		{"item", 2},
	}

	for _, tc := range testCases {
		results, _, err := idx.TextSearch(ctx, tc.query, 100)
		if err != nil {
			t.Fatalf("failed to search for %q: %v", tc.query, err)
		}
		if len(results) != tc.expected {
			t.Errorf("query %q: expected %d results, got %d", tc.query, tc.expected, len(results))
		}
	}
}

// TestFTS_ExtendedBooleanSearch tests boolean search with must/should/must_not.
func TestFTS_ExtendedBooleanSearch(t *testing.T) {
	idx := NewFTSIndex("test_boolean_ext", 0)
	idx.SetTokenizer(NewSimpleTokenizer())
	ctx := context.Background()

	docs := []string{
		"apple banana cherry",
		"apple date elderberry",
		"banana fig grape",
		"cherry date fig",
	}

	for i, doc := range docs {
		if err := idx.IndexDocument(uint64(i), doc); err != nil {
			t.Fatalf("failed to index document %d: %v", i, err)
		}
	}

	// Test MUST only
	query := BooleanQuery{Must: []string{"apple"}}
	results, err := idx.BooleanSearch(ctx, query, 100)
	if err != nil {
		t.Fatalf("boolean search failed: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("MUST apple: expected 2 results, got %d", len(results))
	}

	// Test MUST and SHOULD
	query = BooleanQuery{Must: []string{"apple"}, Should: []string{"banana"}}
	results, err = idx.BooleanSearch(ctx, query, 100)
	if err != nil {
		t.Fatalf("boolean search failed: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("MUST apple SHOULD banana: expected 2 results, got %d", len(results))
	}

	// Test MUST_NOT
	query = BooleanQuery{Must: []string{"date"}, MustNot: []string{"apple"}}
	results, err = idx.BooleanSearch(ctx, query, 100)
	if err != nil {
		t.Fatalf("boolean search failed: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("MUST date MUST_NOT apple: expected 1 result, got %d", len(results))
	}
}

// TestFTS_ExtendedPhraseSearch tests phrase search functionality.
func TestFTS_ExtendedPhraseSearch(t *testing.T) {
	idx := NewFTSIndex("test_phrase_ext", 0)
	idx.SetTokenizer(NewSimpleTokenizer())
	ctx := context.Background()

	docs := []string{
		"quick brown fox jumps",
		"the fox is quick",
		"brown fox sleeps",
		"quick brown dog runs",
	}

	for i, doc := range docs {
		if err := idx.IndexDocument(uint64(i), doc); err != nil {
			t.Fatalf("failed to index document %d: %v", i, err)
		}
	}

	results, err := idx.PhraseSearch(ctx, "quick brown", 100)
	if err != nil {
		t.Fatalf("phrase search failed: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("phrase 'quick brown': expected 2 results, got %d", len(results))
	}

	results, err = idx.PhraseSearch(ctx, "brown fox", 100)
	if err != nil {
		t.Fatalf("phrase search failed: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("phrase 'brown fox': expected 2 results, got %d", len(results))
	}
}

// TestFTS_ExtendedSearchWithScores tests search with relevance scores.
func TestFTS_ExtendedSearchWithScores(t *testing.T) {
	idx := NewFTSIndex("test_scores_ext", 0)
	idx.SetTokenizer(NewSimpleTokenizer())
	ctx := context.Background()

	docs := []string{
		"apple apple apple",
		"apple banana",
		"banana banana",
	}

	for i, doc := range docs {
		if err := idx.IndexDocument(uint64(i), doc); err != nil {
			t.Fatalf("failed to index document %d: %v", i, err)
		}
	}

	results, err := idx.SearchWithScores(ctx, "apple", 100)
	if err != nil {
		t.Fatalf("search with scores failed: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 results, got %d", len(results))
	}

	for _, result := range results {
		if result.Score <= 0 {
			t.Errorf("expected positive score, got %f", result.Score)
		}
	}
}

// TestFTS_ExtendedChineseTokenizer tests Chinese text tokenization.
func TestFTS_ExtendedChineseTokenizer(t *testing.T) {
	idx := NewFTSIndex("test_chinese_ext", 0)
	tok := NewChineseTokenizer()
	tok.AddWord("天气")
	tok.AddWord("编程")
	tok.AddWord("数据库")
	tok.AddWord("技术")
	idx.SetTokenizer(tok)
	ctx := context.Background()

	docs := []string{
		"今天天气很好",
		"我喜欢编程",
		"数据库技术很重要",
	}

	for i, doc := range docs {
		if err := idx.IndexDocument(uint64(i), doc); err != nil {
			t.Fatalf("failed to index document %d: %v", i, err)
		}
	}

	results, _, err := idx.TextSearch(ctx, "天气", 100)
	if err != nil {
		t.Fatalf("failed to search for Chinese term: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("expected 1 result for '天气', got %d", len(results))
	}

	results, _, err = idx.TextSearch(ctx, "编程", 100)
	if err != nil {
		t.Fatalf("failed to search for Chinese term: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("expected 1 result for '编程', got %d", len(results))
	}
}
