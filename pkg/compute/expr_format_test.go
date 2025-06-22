package compute

import (
	"strings"
	"testing"
)

func TestNewExplainOptions(t *testing.T) {
	options := NewExplainOptions()
	if options == nil {
		t.Fatal("NewExplainOptions() returned nil")
	}

	// 验证默认值 - 现在包含Serialize和Format
	expected := "Costs: true, Serialize: none, Timing: true, Format: text"
	if result := options.String(); result != expected {
		t.Errorf("Expected default string '%s', got '%s'", expected, result)
	}
}

func TestExplainOptions_SetBooleanOption(t *testing.T) {
	options := NewExplainOptions()

	// 测试设置布尔选项
	options.SetBooleanOption(ExplainOptionAnalyze, true)
	options.SetBooleanOption(ExplainOptionVerbose, true)
	options.SetBooleanOption(ExplainOptionCosts, false)

	expected := "Analyze: true, Verbose: true, Serialize: none, Timing: true, Format: text"
	if result := options.String(); result != expected {
		t.Errorf("Expected string '%s', got '%s'", expected, result)
	}
}

func TestExplainOptions_SetSerializeOption(t *testing.T) {
	options := NewExplainOptions()

	// 默认已启用Serialize
	options.SetSerializeOption(ExplainOptionSerialize, ExplainSerializeText)

	expected := "Costs: true, Serialize: text, Timing: true, Format: text"
	if result := options.String(); result != expected {
		t.Errorf("Expected string '%s', got '%s'", expected, result)
	}

	// 测试不同的序列化选项
	options.SetSerializeOption(ExplainOptionSerialize, ExplainSerializeBinary)
	expected = "Costs: true, Serialize: binary, Timing: true, Format: text"
	if result := options.String(); result != expected {
		t.Errorf("Expected string '%s', got '%s'", expected, result)
	}

	options.SetSerializeOption(ExplainOptionSerialize, ExplainSerializeNone)
	expected = "Costs: true, Serialize: none, Timing: true, Format: text"
	if result := options.String(); result != expected {
		t.Errorf("Expected string '%s', got '%s'", expected, result)
	}
}

func TestExplainOptions_SetFormatOption(t *testing.T) {
	options := NewExplainOptions()

	// 默认已启用Format
	options.SetFormatOption(ExplainOptionFormat, ExplainFormatJSON)
	expected := "Costs: true, Serialize: none, Timing: true, Format: json"
	if result := options.String(); result != expected {
		t.Errorf("Expected string '%s', got '%s'", expected, result)
	}

	// 测试不同的格式选项
	testCases := []struct {
		format   int
		expected string
	}{
		{ExplainFormatText, "Format: text"},
		{ExplainFormatJSON, "Format: json"},
		{ExplainFormatYAML, "Format: yaml"},
		{ExplainFormatXML, "Format: xml"},
	}

	for _, tc := range testCases {
		options.SetFormatOption(ExplainOptionFormat, tc.format)
		if !strings.Contains(options.String(), tc.expected) {
			t.Errorf("Expected format string '%s' in result, got '%s'", tc.expected, options.String())
		}
	}
}

func TestExplainOptions_String_Empty(t *testing.T) {
	options := &ExplainOptions{}
	// 未设置默认值的空选项应该返回空字符串
	if result := options.String(); result != "" {
		t.Errorf("Expected empty string for uninitialized options, got '%s'", result)
	}
}

func TestExplainOptions_String_MultipleOptions(t *testing.T) {
	options := NewExplainOptions()

	// 设置多个选项
	options.SetBooleanOption(ExplainOptionAnalyze, true)
	options.SetBooleanOption(ExplainOptionVerbose, true)
	options.SetBooleanOption(ExplainOptionBuffers, true)
	options.SetBooleanOption(ExplainOptionMemory, true)
	options.SetSerializeOption(ExplainOptionSerialize, ExplainSerializeText)
	options.SetFormatOption(ExplainOptionFormat, ExplainFormatJSON)

	result := options.String()
	expectedParts := []string{
		"Analyze: true",
		"Verbose: true",
		"Costs: true",
		"Buffers: true",
		"Serialize: text",
		"Timing: true",
		"Memory: true",
		"Format: json",
	}

	for _, part := range expectedParts {
		if !strings.Contains(result, part) {
			t.Errorf("Expected result to contain '%s', got '%s'", part, result)
		}
	}

	// 验证分隔符
	if strings.Count(result, ", ") != 7 { // 应该有7个逗号分隔符
		t.Errorf("Expected 7 comma separators, got %d", strings.Count(result, ", "))
	}
}

func TestExplainOptions_Constants(t *testing.T) {
	// 验证常量值
	if ExplainOptionAnalyze != 1 {
		t.Errorf("Expected ExplainOptionAnalyze to be 1, got %d", ExplainOptionAnalyze)
	}
	if ExplainOptionFormat != 12 {
		t.Errorf("Expected ExplainOptionFormat to be 12, got %d", ExplainOptionFormat)
	}
	if ExplainFormatText != 0 {
		t.Errorf("Expected ExplainFormatText to be 0, got %d", ExplainFormatText)
	}
	if ExplainSerializeNone != 0 {
		t.Errorf("Expected ExplainSerializeNone to be 0, got %d", ExplainSerializeNone)
	}
}

func TestExplainOptionNames(t *testing.T) {
	// 验证选项名称数组
	expectedNames := []string{
		"Invalid Explain Option",
		"Analyze",
		"Verbose",
		"Costs",
		"Settings",
		"GenericPlan",
		"Buffers",
		"Serialize",
		"Wal",
		"Timing",
		"Summary",
		"Memory",
		"Format",
	}

	if len(ExplainOptionNames) != len(expectedNames) {
		t.Errorf("Expected %d option names, got %d", len(expectedNames), len(ExplainOptionNames))
	}

	for i, expected := range expectedNames {
		if i < len(ExplainOptionNames) && ExplainOptionNames[i] != expected {
			t.Errorf("Expected option name at index %d to be '%s', got '%s'", i, expected, ExplainOptionNames[i])
		}
	}
}

func TestExplainOptions_SetSerializeOption_WithoutEnable(t *testing.T) {
	options := &ExplainOptions{} // 不启用Serialize
	options.SetSerializeOption(ExplainOptionSerialize, ExplainSerializeText)
	expected := ""
	if result := options.String(); result != expected {
		t.Errorf("Expected string '%s', got '%s'", expected, result)
	}
}

func TestExplainOptions_SetFormatOption_WithoutEnable(t *testing.T) {
	options := &ExplainOptions{} // 不启用Format
	options.SetFormatOption(ExplainOptionFormat, ExplainFormatJSON)
	expected := ""
	if result := options.String(); result != expected {
		t.Errorf("Expected string '%s', got '%s'", expected, result)
	}
}

func TestExplainBuffer_NewBuffer(t *testing.T) {
	buffer := NewExplainBuffer()
	if buffer.lines == nil {
		t.Error("Expected lines slice to be initialized")
	}
	if len(buffer.lines) != 0 {
		t.Errorf("Expected empty buffer, got %d lines", len(buffer.lines))
	}
}

func TestExplainBuffer_AppendTitle(t *testing.T) {
	buffer := NewExplainBuffer()

	// 测试添加标题
	buffer.AppendTitle("Test Title")

	if len(buffer.lines) != 1 {
		t.Errorf("Expected 1 line, got %d", len(buffer.lines))
	}
	if buffer.lines[0] != "Test Title" {
		t.Errorf("Expected 'Test Title', got '%s'", buffer.lines[0])
	}

	// 测试添加多个标题
	buffer.AppendTitle("Another Title")

	if len(buffer.lines) != 2 {
		t.Errorf("Expected 2 lines, got %d", len(buffer.lines))
	}
	if buffer.lines[1] != "Another Title" {
		t.Errorf("Expected 'Another Title', got '%s'", buffer.lines[1])
	}
}

func TestExplainBuffer_AppendLine_Level0(t *testing.T) {
	buffer := NewExplainBuffer()

	// 测试level 0, newNode = true
	buffer.AppendLine("Test Line", 0, true)
	expected := "Test Line"
	if buffer.lines[0] != expected {
		t.Errorf("Expected '%s', got '%s'", expected, buffer.lines[0])
	}

	// 测试level 0, newNode = false
	buffer.AppendLine("Indented Line", 0, false)
	expected = "  Indented Line"
	if buffer.lines[1] != expected {
		t.Errorf("Expected '%s', got '%s'", expected, buffer.lines[1])
	}
}

func TestExplainBuffer_AppendLine_Level1(t *testing.T) {
	buffer := NewExplainBuffer()

	// 测试level 1, newNode = true
	buffer.AppendLine("Level 1 New", 1, true)
	expected := "->  Level 1 New"
	if buffer.lines[0] != expected {
		t.Errorf("Expected '%s', got '%s'", expected, buffer.lines[0])
	}

	// 测试level 1, newNode = false
	buffer.AppendLine("Level 1 Continue", 1, false)
	expected = "  Level 1 Continue"
	if buffer.lines[1] != expected {
		t.Errorf("Expected '%s', got '%s'", expected, buffer.lines[1])
	}
}

func TestExplainBuffer_AppendLine_Level2(t *testing.T) {
	buffer := NewExplainBuffer()

	// 测试level 2, newNode = true
	buffer.AppendLine("Level 2 New", 2, true)
	expected := "  ->  Level 2 New"
	if buffer.lines[0] != expected {
		t.Errorf("Expected '%s', got '%s'", expected, buffer.lines[0])
	}

	// 测试level 2, newNode = false
	buffer.AppendLine("Level 2 Continue", 2, false)
	expected = "        Level 2 Continue"
	if buffer.lines[1] != expected {
		t.Errorf("Expected '%s', got '%s'", expected, buffer.lines[1])
	}
}

func TestExplainBuffer_AppendLine_NegativeLevel(t *testing.T) {
	buffer := NewExplainBuffer()

	// 测试负数level
	buffer.AppendLine("Negative Level", -1, true)
	expected := "Negative Level"
	if buffer.lines[0] != expected {
		t.Errorf("Expected '%s', got '%s'", expected, buffer.lines[0])
	}

	buffer.AppendLine("Negative Level Continue", -1, false)
	expected = "  Negative Level Continue"
	if buffer.lines[1] != expected {
		t.Errorf("Expected '%s', got '%s'", expected, buffer.lines[1])
	}
}

func TestExplainBuffer_GetOffset(t *testing.T) {
	buffer := NewExplainBuffer()

	testCases := []struct {
		level  int
		offset int
	}{
		{-1, 2},
		{0, 2},
		{1, 2},
		{2, 8},
		{3, 14},
		{4, 20},
	}

	for _, tc := range testCases {
		result := buffer.getOffset(tc.level)
		if result != tc.offset {
			t.Errorf("For level %d, expected offset %d, got %d", tc.level, tc.offset, result)
		}
	}
}

func TestExplainBuffer_String(t *testing.T) {
	buffer := NewExplainBuffer()

	// 测试空buffer
	result := buffer.String()
	if result != "" {
		t.Errorf("Expected empty string, got '%s'", result)
	}

	// 测试单行
	buffer.AppendTitle("Single Line")
	result = buffer.String()
	expected := "Single Line"
	if result != expected {
		t.Errorf("Expected '%s', got '%s'", expected, result)
	}

	// 测试多行
	buffer.AppendLine("Level 1", 1, true)
	buffer.AppendLine("Level 2", 2, false)
	result = buffer.String()
	expected = "Single Line\n->  Level 1\n        Level 2"
	if result != expected {
		t.Errorf("Expected '%s', got '%s'", expected, result)
	}
}

func TestExplainBuffer_ComplexScenario(t *testing.T) {
	buffer := NewExplainBuffer()

	// 模拟一个复杂的解释计划结构
	buffer.AppendTitle("EXPLAIN")
	buffer.AppendLine("Seq Scan on users", 1, true)
	buffer.AppendLine("Filter: (age > 18)", 1, false)
	buffer.AppendLine("Sort", 1, true)
	buffer.AppendLine("Sort Key: name", 2, true)
	buffer.AppendLine("Sort Method: quicksort", 2, false)

	result := buffer.String()
	expected := `EXPLAIN
->  Seq Scan on users
  Filter: (age > 18)
->  Sort
  ->  Sort Key: name
        Sort Method: quicksort`

	if result != expected {
		t.Errorf("Expected:\n%s\nGot:\n%s", expected, result)
	}
}

func TestExplainBuffer_EmptyLines(t *testing.T) {
	buffer := NewExplainBuffer()

	// 测试空字符串
	buffer.AppendTitle("")
	buffer.AppendLine("", 0, true)
	buffer.AppendLine("", 1, false)

	result := buffer.String()
	expected := "\n\n  "
	if result != expected {
		t.Errorf("Expected '%s', got '%s'", expected, result)
	}
}
