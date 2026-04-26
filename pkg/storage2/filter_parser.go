package storage2

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
)

// FilterExpr represents a parsed filter expression
type FilterExpr struct {
	// Predicate is the compiled predicate
	Predicate FilterPredicate
	// Original is the original filter string
	Original string
}

// FilterParser parses filter expressions into FilterPredicate
type FilterParser struct {
	tokens  []token
	pos     int
	columns []string // column name to index mapping
}

type token struct {
	typ   tokenType
	value string
}

type tokenType int

const (
	tokenEOF tokenType = iota
	tokenIdent
	tokenNumber
	tokenString
	tokenOp
	tokenLParen
	tokenRParen
	tokenComma
)

// NewFilterParser creates a new filter parser
func NewFilterParser(filter string) *FilterParser {
	return &FilterParser{
		tokens: tokenize(filter),
	}
}

// WithColumns sets the column name to index mapping
func (p *FilterParser) WithColumns(columns []string) *FilterParser {
	p.columns = columns
	return p
}

// Parse parses the filter expression into a FilterPredicate
func (p *FilterParser) Parse() (FilterPredicate, error) {
	if len(p.tokens) == 0 || (len(p.tokens) == 1 && p.tokens[0].typ == tokenEOF) {
		return nil, nil // empty filter
	}
	return p.parseOr()
}

// parseOr parses OR expressions (lowest precedence)
func (p *FilterParser) parseOr() (FilterPredicate, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}

	for p.peek().value == "OR" {
		p.advance() // consume OR
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = NewOrPredicate(left, right)
	}

	return left, nil
}

// parseAnd parses AND expressions
func (p *FilterParser) parseAnd() (FilterPredicate, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}

	for p.peek().value == "AND" {
		p.advance() // consume AND
		right, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		left = NewAndPredicate(left, right)
	}

	return left, nil
}

// parseNot parses NOT expressions
func (p *FilterParser) parseNot() (FilterPredicate, error) {
	if p.peek().value == "NOT" {
		p.advance() // consume NOT
		inner, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		return NewNotPredicate(inner), nil
	}
	return p.parsePrimary()
}

// parsePrimary parses primary expressions (comparisons, parenthesized expressions)
func (p *FilterParser) parsePrimary() (FilterPredicate, error) {
	tok := p.peek()

	if tok.typ == tokenLParen {
		p.advance() // consume (
		inner, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		if p.peek().typ != tokenRParen {
			return nil, fmt.Errorf("expected ')', got %s", p.peek().value)
		}
		p.advance() // consume )
		return inner, nil
	}

	// Parse comparison: column op value
	return p.parseComparison()
}

// parseComparison parses a comparison expression
func (p *FilterParser) parseComparison() (FilterPredicate, error) {
	// Get column
	tok := p.peek()
	if tok.typ != tokenIdent {
		return nil, fmt.Errorf("expected column name, got %q", tok.value)
	}
	columnName := tok.value
	p.advance()

	// Handle IS NULL / IS NOT NULL
	if p.peek().value == "IS" {
		p.advance() // consume IS
		if p.peek().value == "NOT" {
			p.advance() // consume NOT
			if p.peek().value != "NULL" {
				return nil, fmt.Errorf("expected NULL after IS NOT")
			}
			p.advance() // consume NULL
			// Return a NOT NULL predicate
			colIdx, err := p.getColumnIndex(columnName)
			if err != nil {
				return nil, err
			}
			return &NotNullPredicate{ColumnIndex: colIdx}, nil
		}
		if p.peek().value != "NULL" {
			return nil, fmt.Errorf("expected NULL after IS")
		}
		p.advance() // consume NULL
		colIdx, err := p.getColumnIndex(columnName)
		if err != nil {
			return nil, err
		}
		return &NullPredicate{ColumnIndex: colIdx}, nil
	}

	// Handle IN expression
	if strings.ToUpper(p.peek().value) == "IN" {
		p.advance() // consume IN
		return p.parseInExpression(columnName)
	}

	// Handle LIKE expression
	if strings.ToUpper(p.peek().value) == "LIKE" {
		p.advance() // consume LIKE
		pattern := p.peek()
		if pattern.typ != tokenString {
			return nil, fmt.Errorf("expected string pattern after LIKE, got %q", pattern.value)
		}
		p.advance() // consume pattern
		colIdx, err := p.getColumnIndex(columnName)
		if err != nil {
			return nil, err
		}
		return &LikePredicate{ColumnIndex: colIdx, Pattern: pattern.value}, nil
	}

	// Get operator
	opTok := p.peek()
	if opTok.typ != tokenOp {
		return nil, fmt.Errorf("expected operator, got %q", opTok.value)
	}
	op := opTok.value
	p.advance()

	// Get value
	valTok := p.peek()
	value, err := p.parseValue(valTok)
	if err != nil {
		return nil, err
	}
	p.advance()

	colIdx, err := p.getColumnIndex(columnName)
	if err != nil {
		return nil, err
	}

	compOp, err := parseComparisonOp(op)
	if err != nil {
		return nil, err
	}

	return NewColumnPredicate(colIdx, compOp, value), nil
}

// parseInExpression parses an IN expression
func (p *FilterParser) parseInExpression(columnName string) (FilterPredicate, error) {
	if p.peek().typ != tokenLParen {
		return nil, fmt.Errorf("expected '(' after IN")
	}
	p.advance() // consume (

	var values []*chunk.Value
	for {
		valTok := p.peek()
		val, err := p.parseValue(valTok)
		if err != nil {
			return nil, err
		}
		values = append(values, val)
		p.advance()

		if p.peek().typ == tokenRParen {
			break
		}
		if p.peek().typ != tokenComma {
			return nil, fmt.Errorf("expected ',' or ')' in IN list")
		}
		p.advance() // consume ,
	}
	p.advance() // consume )

	colIdx, err := p.getColumnIndex(columnName)
	if err != nil {
		return nil, err
	}

	return &InPredicate{ColumnIndex: colIdx, Values: values}, nil
}

// parseValue parses a literal value
func (p *FilterParser) parseValue(tok token) (*chunk.Value, error) {
	switch tok.typ {
	case tokenNumber:
		// Try integer first
		if i, err := strconv.ParseInt(tok.value, 10, 64); err == nil {
			return &chunk.Value{
				Typ: common.LType{Id: common.LTID_BIGINT},
				I64: i,
			}, nil
		}
		// Try float
		if f, err := strconv.ParseFloat(tok.value, 64); err == nil {
			return &chunk.Value{
				Typ: common.LType{Id: common.LTID_DOUBLE},
				F64: f,
			}, nil
		}
		return nil, fmt.Errorf("invalid number: %s", tok.value)
	case tokenString:
		// Remove quotes
		s := tok.value
		if len(s) >= 2 && (s[0] == '\'' || s[0] == '"') {
			s = s[1 : len(s)-1]
		}
		return &chunk.Value{
			Typ: common.LType{Id: common.LTID_VARCHAR},
			Str: s,
		}, nil
	case tokenIdent:
		// Handle true/false/null
		switch strings.ToUpper(tok.value) {
		case "TRUE":
			return &chunk.Value{
				Typ:  common.LType{Id: common.LTID_BOOLEAN},
				Bool: true,
			}, nil
		case "FALSE":
			return &chunk.Value{
				Typ:  common.LType{Id: common.LTID_BOOLEAN},
				Bool: false,
			}, nil
		case "NULL":
			return nil, nil // NULL value
		}
		return nil, fmt.Errorf("unknown identifier: %s", tok.value)
	default:
		return nil, fmt.Errorf("expected value, got %q", tok.value)
	}
}

// getColumnIndex resolves a column name to its index
func (p *FilterParser) getColumnIndex(name string) (int, error) {
	// Handle c0, c1, ... format
	if len(name) > 1 && name[0] == 'c' {
		if idx, err := strconv.Atoi(name[1:]); err == nil {
			return idx, nil
		}
	}

	// Look up in column mapping
	if p.columns != nil {
		for i, col := range p.columns {
			if col == name {
				return i, nil
			}
		}
	}

	return 0, fmt.Errorf("unknown column: %s", name)
}

func (p *FilterParser) peek() token {
	if p.pos >= len(p.tokens) {
		return token{typ: tokenEOF}
	}
	return p.tokens[p.pos]
}

func (p *FilterParser) advance() {
	p.pos++
}

// parseComparisonOp converts a string operator to ComparisonOp
func parseComparisonOp(op string) (ComparisonOp, error) {
	switch op {
	case "=", "==":
		return Eq, nil
	case "!=", "<>":
		return Ne, nil
	case "<":
		return Lt, nil
	case "<=":
		return Le, nil
	case ">":
		return Gt, nil
	case ">=":
		return Ge, nil
	default:
		return Eq, fmt.Errorf("unknown operator: %s", op)
	}
}

// tokenize converts a filter string into tokens
func tokenize(s string) []token {
	var tokens []token
	i := 0

	for i < len(s) {
		// Skip whitespace
		for i < len(s) && unicode.IsSpace(rune(s[i])) {
			i++
		}
		if i >= len(s) {
			break
		}

		ch := s[i]

		// String literal
		if ch == '\'' || ch == '"' {
			quote := ch
			start := i
			i++
			for i < len(s) && s[i] != quote {
				if s[i] == '\\' {
					i++
				}
				i++
			}
			if i < len(s) {
				i++
			}
			tokens = append(tokens, token{typ: tokenString, value: s[start:i]})
			continue
		}

		// Number
		if unicode.IsDigit(rune(ch)) || (ch == '-' && i+1 < len(s) && unicode.IsDigit(rune(s[i+1]))) {
			start := i
			if ch == '-' {
				i++
			}
			for i < len(s) && (unicode.IsDigit(rune(s[i])) || s[i] == '.') {
				i++
			}
			tokens = append(tokens, token{typ: tokenNumber, value: s[start:i]})
			continue
		}

		// Identifier or keyword
		if unicode.IsLetter(rune(ch)) || ch == '_' {
			start := i
			for i < len(s) && (unicode.IsLetter(rune(s[i])) || unicode.IsDigit(rune(s[i])) || s[i] == '_') {
				i++
			}
			tokens = append(tokens, token{typ: tokenIdent, value: s[start:i]})
			continue
		}

		// Operators
		if ch == '<' || ch == '>' || ch == '=' || ch == '!' {
			start := i
			i++
			if i < len(s) && (s[i] == '=' || (ch == '<' && s[i] == '>')) {
				i++
			}
			tokens = append(tokens, token{typ: tokenOp, value: s[start:i]})
			continue
		}

		// Single character tokens
		switch ch {
		case '(':
			tokens = append(tokens, token{typ: tokenLParen, value: "("})
		case ')':
			tokens = append(tokens, token{typ: tokenRParen, value: ")"})
		case ',':
			tokens = append(tokens, token{typ: tokenComma, value: ","})
		}
		i++
	}

	tokens = append(tokens, token{typ: tokenEOF})
	return tokens
}

// NullPredicate checks if a value is NULL
type NullPredicate struct {
	ColumnIndex int
}

func (p *NullPredicate) Evaluate(c *chunk.Chunk) ([]bool, error) {
	if p.ColumnIndex < 0 || p.ColumnIndex >= c.ColumnCount() {
		return nil, fmt.Errorf("column index %d out of range", p.ColumnIndex)
	}

	col := c.Data[p.ColumnIndex]
	result := make([]bool, c.Card())

	for i := 0; i < c.Card(); i++ {
		val := col.GetValue(i)
		result[i] = val == nil || val.IsNull
	}

	return result, nil
}

func (p *NullPredicate) CanPushdown() bool { return true }
func (p *NullPredicate) String() string    { return fmt.Sprintf("col[%d] IS NULL", p.ColumnIndex) }

// NotNullPredicate checks if a value is NOT NULL
type NotNullPredicate struct {
	ColumnIndex int
}

func (p *NotNullPredicate) Evaluate(c *chunk.Chunk) ([]bool, error) {
	if p.ColumnIndex < 0 || p.ColumnIndex >= c.ColumnCount() {
		return nil, fmt.Errorf("column index %d out of range", p.ColumnIndex)
	}

	col := c.Data[p.ColumnIndex]
	result := make([]bool, c.Card())

	for i := 0; i < c.Card(); i++ {
		val := col.GetValue(i)
		result[i] = val != nil && !val.IsNull
	}

	return result, nil
}

func (p *NotNullPredicate) CanPushdown() bool { return true }
func (p *NotNullPredicate) String() string    { return fmt.Sprintf("col[%d] IS NOT NULL", p.ColumnIndex) }

// InPredicate checks if a value is in a list
type InPredicate struct {
	ColumnIndex int
	Values      []*chunk.Value
}

func (p *InPredicate) Evaluate(c *chunk.Chunk) ([]bool, error) {
	if p.ColumnIndex < 0 || p.ColumnIndex >= c.ColumnCount() {
		return nil, fmt.Errorf("column index %d out of range", p.ColumnIndex)
	}

	col := c.Data[p.ColumnIndex]
	result := make([]bool, c.Card())

	for i := 0; i < c.Card(); i++ {
		val := col.GetValue(i)
		result[i] = false
		for _, v := range p.Values {
			if compareValues(val, v, Eq) {
				result[i] = true
				break
			}
		}
	}

	return result, nil
}

func (p *InPredicate) CanPushdown() bool { return true }
func (p *InPredicate) String() string {
	return fmt.Sprintf("col[%d] IN (...)", p.ColumnIndex)
}

// LikePredicate checks if a string matches a pattern
type LikePredicate struct {
	ColumnIndex int
	Pattern     string
}

func (p *LikePredicate) Evaluate(c *chunk.Chunk) ([]bool, error) {
	if p.ColumnIndex < 0 || p.ColumnIndex >= c.ColumnCount() {
		return nil, fmt.Errorf("column index %d out of range", p.ColumnIndex)
	}

	col := c.Data[p.ColumnIndex]
	result := make([]bool, c.Card())

	// Convert SQL LIKE pattern to simple matching
	// % matches any sequence, _ matches single character
	regex := likeToRegex(p.Pattern)

	for i := 0; i < c.Card(); i++ {
		val := col.GetValue(i)
		if val == nil {
			result[i] = false
			continue
		}
		str := val.Str
		result[i] = matchLike(str, regex)
	}

	return result, nil
}

func (p *LikePredicate) CanPushdown() bool { return true }
func (p *LikePredicate) String() string {
	return fmt.Sprintf("col[%d] LIKE %q", p.ColumnIndex, p.Pattern)
}

// likeToRegex converts a SQL LIKE pattern to a simple matching structure
func likeToRegex(pattern string) []likeSegment {
	var segments []likeSegment
	i := 0

	for i < len(pattern) {
		if pattern[i] == '%' {
			segments = append(segments, likeSegment{typ: likeAny})
			i++
		} else if pattern[i] == '_' {
			segments = append(segments, likeSegment{typ: likeOne})
			i++
		} else {
			start := i
			for i < len(pattern) && pattern[i] != '%' && pattern[i] != '_' {
				i++
			}
			segments = append(segments, likeSegment{typ: likeLiteral, text: pattern[start:i]})
		}
	}

	return segments
}

type likeSegmentType int

const (
	likeLiteral likeSegmentType = iota
	likeAny
	likeOne
)

type likeSegment struct {
	typ  likeSegmentType
	text string
}

// matchLike matches a string against a LIKE pattern
func matchLike(s string, segments []likeSegment) bool {
	return matchLikeRecursive(s, segments, 0, 0)
}

func matchLikeRecursive(s string, segments []likeSegment, si int, segIdx int) bool {
	// If we've processed all segments, we should have consumed all of s
	if segIdx >= len(segments) {
		return si >= len(s)
	}

	seg := segments[segIdx]

	switch seg.typ {
	case likeLiteral:
		if si+len(seg.text) > len(s) {
			return false
		}
		if s[si:si+len(seg.text)] != seg.text {
			return false
		}
		return matchLikeRecursive(s, segments, si+len(seg.text), segIdx+1)

	case likeOne:
		if si >= len(s) {
			return false
		}
		return matchLikeRecursive(s, segments, si+1, segIdx+1)

	case likeAny:
		// % can match zero or more characters
		// Try matching zero characters first, then more
		for i := si; i <= len(s); i++ {
			if matchLikeRecursive(s, segments, i, segIdx+1) {
				return true
			}
		}
		return false
	}

	return false
}

// ParseFilter parses a filter string into a FilterPredicate
func ParseFilter(filter string, columns []string) (FilterPredicate, error) {
	parser := NewFilterParser(filter).WithColumns(columns)
	return parser.Parse()
}

// CountRowsWithFilter counts rows matching a filter predicate
func CountRowsWithFilter(ctx context.Context, basePath string, handler CommitHandler, version uint64, filter FilterPredicate) (uint64, error) {
	m, err := LoadManifest(ctx, basePath, handler, version)
	if err != nil {
		return 0, err
	}

	var total uint64
	for _, frag := range m.Fragments {
		if frag == nil {
			continue
		}

		for _, df := range frag.Files {
			if df == nil || df.Path == "" {
				continue
			}

			// Read chunk from file
			fullPath := basePath + "/" + df.Path
			c, err := ReadChunkFromFile(fullPath)
			if err != nil {
				return 0, fmt.Errorf("failed to read chunk %s: %w", fullPath, err)
			}

			// Apply filter
			if filter != nil {
				mask, err := filter.Evaluate(c)
				if err != nil {
					return 0, err
				}
				for _, m := range mask {
					if m {
						total++
					}
				}
			} else {
				total += uint64(c.Card())
			}
		}
	}

	return total, nil
}
