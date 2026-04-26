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

	options.SetSerializeOption(ExplainOptionSerialize, ExplainSerializeText)

	expected := "Costs: true, Serialize: text, Timing: true, Format: text"
	if result := options.String(); result != expected {
		t.Errorf("Expected string '%s', got '%s'", expected, result)
	}

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

	options.SetFormatOption(ExplainOptionFormat, ExplainFormatJSON)
	expected := "Costs: true, Serialize: none, Timing: true, Format: json"
	if result := options.String(); result != expected {
		t.Errorf("Expected string '%s', got '%s'", expected, result)
	}

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

func TestExplainOptions_Constants(t *testing.T) {
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
