package btree

import (
	"testing"
	"unsafe"

	"github.com/daviszhen/plan/pkg/util"
)

// MockTuple is a mock implementation of Tuple for testing
type MockTuple struct {
	data        []byte
	formatFlags uint8
}

// MockBTreeOps is a mock implementation of BTreeOps for testing
type MockBTreeOps struct {
	lenFunc          func(desc *BTDesc, tuple Tuple, lenType LengthType) int
	tupleMakeKeyFunc func(desc *BTDesc, tuple Tuple, data unsafe.Pointer, keepVersion bool, allocated *bool) Tuple
}

func (m *MockBTreeOps) Len(desc *BTDesc, tuple Tuple, lenType LengthType) int {
	return m.lenFunc(desc, tuple, lenType)
}

func (m *MockBTreeOps) TupleMakeKey(desc *BTDesc, tuple Tuple, data unsafe.Pointer, keepVersion bool, allocated *bool) Tuple {
	return m.tupleMakeKeyFunc(desc, tuple, data, keepVersion, allocated)
}

func TestBTreeOps(t *testing.T) {
	tests := []struct {
		name             string
		lenFunc          func(desc *BTDesc, tuple Tuple, lenType LengthType) int
		tupleMakeKeyFunc func(desc *BTDesc, tuple Tuple, data unsafe.Pointer, keepVersion bool, allocated *bool) Tuple
		tuple            Tuple
		lenType          LengthType
		expectedLen      int
	}{
		{
			name: "Test basic length calculation",
			lenFunc: func(desc *BTDesc, tuple Tuple, lenType LengthType) int {
				return 4 // Mock length for testing
			},
			tupleMakeKeyFunc: func(desc *BTDesc, tuple Tuple, data unsafe.Pointer, keepVersion bool, allocated *bool) Tuple {
				return tuple
			},
			tuple:       Tuple{data: unsafe.Pointer(&[]byte("test")[0]), formatFlags: 0},
			lenType:     TupleLength,
			expectedLen: 4,
		},
		{
			name: "Test key length calculation",
			lenFunc: func(desc *BTDesc, tuple Tuple, lenType LengthType) int {
				if lenType == KeyLength {
					return 3 // Mock key length for testing
				}
				return 4 // Mock tuple length for testing
			},
			tupleMakeKeyFunc: func(desc *BTDesc, tuple Tuple, data unsafe.Pointer, keepVersion bool, allocated *bool) Tuple {
				return tuple
			},
			tuple:       Tuple{data: unsafe.Pointer(&[]byte("test")[0]), formatFlags: 0},
			lenType:     KeyLength,
			expectedLen: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockOps := &MockBTreeOps{
				lenFunc:          tt.lenFunc,
				tupleMakeKeyFunc: tt.tupleMakeKeyFunc,
			}

			// Test Len method
			desc := &BTDesc{ops: mockOps}
			actualLen := mockOps.Len(desc, tt.tuple, tt.lenType)
			if actualLen != tt.expectedLen {
				t.Errorf("Len() = %v, want %v", actualLen, tt.expectedLen)
			}

			// Test TupleMakeKey method
			var allocated bool
			result := mockOps.TupleMakeKey(desc, tt.tuple, tt.tuple.data, false, &allocated)
			if result.data != tt.tuple.data || result.formatFlags != tt.tuple.formatFlags {
				t.Errorf("TupleMakeKey() = %v, want %v", result, tt.tuple)
			}
		})
	}
}

func TestBTLen(t *testing.T) {
	mockOps := &MockBTreeOps{
		lenFunc: func(desc *BTDesc, tuple Tuple, lenType LengthType) int {
			return 4 // Mock length for testing
		},
		tupleMakeKeyFunc: func(desc *BTDesc, tuple Tuple, data unsafe.Pointer, keepVersion bool, allocated *bool) Tuple {
			return tuple
		},
	}

	desc := &BTDesc{ops: mockOps}
	tuple := Tuple{data: unsafe.Pointer(&[]byte("test")[0]), formatFlags: 0}

	// Test BTLen function
	actualLen := BTLen(desc, tuple, TupleLength)
	expectedLen := 4
	if actualLen != expectedLen {
		t.Errorf("BTLen() = %v, want %v", actualLen, expectedLen)
	}
}

func TestBTTupleMakeKey(t *testing.T) {
	mockOps := &MockBTreeOps{
		lenFunc: func(desc *BTDesc, tuple Tuple, lenType LengthType) int {
			return 4 // Mock length for testing
		},
		tupleMakeKeyFunc: func(desc *BTDesc, tuple Tuple, data unsafe.Pointer, keepVersion bool, allocated *bool) Tuple {
			return tuple
		},
	}

	desc := &BTDesc{ops: mockOps}
	tuple := Tuple{data: unsafe.Pointer(&[]byte("test")[0]), formatFlags: 0}
	var allocated bool

	// Test BTTupleMakeKey function
	result := BTTupleMakeKey(desc, tuple, tuple.data, false, &allocated)
	if result.data != tuple.data || result.formatFlags != tuple.formatFlags {
		t.Errorf("BTTupleMakeKey() = %v, want %v", result, tuple)
	}
}

func TestTupleIsNULL(t *testing.T) {
	tests := []struct {
		name     string
		tuple    Tuple
		expected bool
	}{
		{
			name:     "Test NULL tuple",
			tuple:    Tuple{data: nil, formatFlags: 0},
			expected: true,
		},
		{
			name:     "Test non-NULL tuple",
			tuple:    Tuple{data: unsafe.Pointer(&[]byte("test")[0]), formatFlags: 0},
			expected: false,
		},
		{
			name:     "Test tuple with format flags but NULL data",
			tuple:    Tuple{data: nil, formatFlags: 1},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := TupleIsNULL(tt.tuple)
			if result != tt.expected {
				t.Errorf("TupleIsNULL() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestTupleSetNULL(t *testing.T) {
	tests := []struct {
		name     string
		tuple    Tuple
		expected Tuple
	}{
		{
			name:     "Test setting NULL on non-NULL tuple",
			tuple:    Tuple{data: unsafe.Pointer(&[]byte("test")[0]), formatFlags: 1},
			expected: Tuple{data: nil, formatFlags: 0},
		},
		{
			name:     "Test setting NULL on already NULL tuple",
			tuple:    Tuple{data: nil, formatFlags: 0},
			expected: Tuple{data: nil, formatFlags: 0},
		},
		{
			name:     "Test setting NULL on tuple with format flags",
			tuple:    Tuple{data: unsafe.Pointer(&[]byte("test")[0]), formatFlags: 0xFF},
			expected: Tuple{data: nil, formatFlags: 0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a copy of the tuple to modify
			tuple := tt.tuple
			TupleSetNULL(&tuple)

			// Check if the tuple was properly set to NULL
			if tuple.data != nil {
				t.Errorf("TupleSetNULL() data = %v, want nil", tuple.data)
			}
			if tuple.formatFlags != 0 {
				t.Errorf("TupleSetNULL() formatFlags = %v, want 0", tuple.formatFlags)
			}
		})
	}
}

func TestClearFixedKey(t *testing.T) {
	tests := []struct {
		name     string
		key      FixedKey
		expected FixedKey
	}{
		{
			name: "Test clearing non-empty fixed key",
			key: FixedKey{
				tuple: Tuple{
					data:        unsafe.Pointer(&[]byte("test")[0]),
					formatFlags: 0xFF,
				},
			},
			expected: FixedKey{
				tuple: Tuple{
					data:        nil,
					formatFlags: 0,
				},
			},
		},
		{
			name: "Test clearing already empty fixed key",
			key: FixedKey{
				tuple: Tuple{
					data:        nil,
					formatFlags: 0,
				},
			},
			expected: FixedKey{
				tuple: Tuple{
					data:        nil,
					formatFlags: 0,
				},
			},
		},
		{
			name: "Test clearing fixed key with only format flags",
			key: FixedKey{
				tuple: Tuple{
					data:        nil,
					formatFlags: 0x55,
				},
			},
			expected: FixedKey{
				tuple: Tuple{
					data:        nil,
					formatFlags: 0,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a copy of the key to modify
			key := tt.key
			clearFixedKey(&key)

			// Check if the key was properly cleared
			if key.tuple.data != nil {
				t.Errorf("clearFixedKey() data = %v, want nil", key.tuple.data)
			}
			if key.tuple.formatFlags != 0 {
				t.Errorf("clearFixedKey() formatFlags = %v, want 0", key.tuple.formatFlags)
			}
		})
	}
}

func TestCopyFixedKeyWithLen(t *testing.T) {
	tests := []struct {
		name     string
		src      Tuple
		tuplen   int
		expected FixedKey
	}{
		{
			name: "Test copying NULL tuple",
			src: Tuple{
				data:        nil,
				formatFlags: 0,
			},
			tuplen: 0,
			expected: FixedKey{
				tuple: Tuple{
					data:        nil,
					formatFlags: 0,
				},
			},
		},
		{
			name: "Test copying non-NULL tuple with aligned length",
			src: Tuple{
				data:        unsafe.Pointer(&[]byte("test")[0]),
				formatFlags: 0x55,
			},
			tuplen: 4,
			expected: FixedKey{
				tuple: Tuple{
					data:        unsafe.Pointer(&[]byte("test")[0]),
					formatFlags: 0x55,
				},
			},
		},
		{
			name: "Test copying non-NULL tuple with unaligned length",
			src: Tuple{
				data:        unsafe.Pointer(&[]byte("test123")[0]),
				formatFlags: 0xFF,
			},
			tuplen: 7,
			expected: FixedKey{
				tuple: Tuple{
					data:        unsafe.Pointer(&[]byte("test123")[0]),
					formatFlags: 0xFF,
				},
			},
		},
		{
			name: "Test copying tuple with maximum length",
			src: Tuple{
				data:        unsafe.Pointer(&[]byte("test123456789012")[0]),
				formatFlags: 0xAA,
			},
			tuplen: 15,
			expected: FixedKey{
				tuple: Tuple{
					data:        unsafe.Pointer(&[]byte("test123456789012")[0]),
					formatFlags: 0xAA,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a destination key
			var dst FixedKey

			// Copy the source tuple to the destination key
			copyFixedKeyWithLen(&dst, tt.src, tt.tuplen)

			// For NULL tuple, verify the key is cleared
			if TupleIsNULL(tt.src) {
				if dst.tuple.data != nil {
					t.Errorf("copyFixedKeyWithLen() data = %v, want nil for NULL source", dst.tuple.data)
				}
				if dst.tuple.formatFlags != 0 {
					t.Errorf("copyFixedKeyWithLen() formatFlags = %v, want 0 for NULL source", dst.tuple.formatFlags)
				}
				return
			}

			// For non-NULL tuple, verify the copy
			if dst.tuple.formatFlags != tt.src.formatFlags {
				t.Errorf("copyFixedKeyWithLen() formatFlags = %v, want %v", dst.tuple.formatFlags, tt.src.formatFlags)
			}

			// Verify the data was copied correctly
			if dst.tuple.data == nil {
				t.Error("copyFixedKeyWithLen() data is nil for non-NULL source")
			} else {
				// Compare the copied data with the source data
				srcData := (*[1 << 30]byte)(tt.src.data)[:tt.tuplen:tt.tuplen]
				dstData := (*[1 << 30]byte)(dst.tuple.data)[:tt.tuplen:tt.tuplen]
				for i := 0; i < tt.tuplen; i++ {
					if srcData[i] != dstData[i] {
						t.Errorf("copyFixedKeyWithLen() data[%d] = %v, want %v", i, dstData[i], srcData[i])
					}
				}

				// Verify padding bytes are zeroed
				alignedLen := util.AlignValue(tt.tuplen, 8)
				if tt.tuplen != alignedLen {
					dstData = (*[1 << 30]byte)(dst.tuple.data)[tt.tuplen:alignedLen]
					for i := 0; i < len(dstData); i++ {
						if dstData[i] != 0 {
							t.Errorf("copyFixedKeyWithLen() padding[%d] = %v, want 0", i, dstData[i])
						}
					}
				}
			}
		})
	}
}

func TestCopyFixedKey(t *testing.T) {
	tests := []struct {
		name     string
		src      Tuple
		lenFunc  func(desc *BTDesc, tuple Tuple, lenType LengthType) int
		expected FixedKey
	}{
		{
			name: "Test copying NULL tuple",
			src: Tuple{
				data:        nil,
				formatFlags: 0,
			},
			lenFunc: func(desc *BTDesc, tuple Tuple, lenType LengthType) int {
				return 0
			},
			expected: FixedKey{
				tuple: Tuple{
					data:        nil,
					formatFlags: 0,
				},
			},
		},
		{
			name: "Test copying non-NULL tuple with short length",
			src: Tuple{
				data:        unsafe.Pointer(&[]byte("test")[0]),
				formatFlags: 0x55,
			},
			lenFunc: func(desc *BTDesc, tuple Tuple, lenType LengthType) int {
				return 4
			},
			expected: FixedKey{
				tuple: Tuple{
					data:        unsafe.Pointer(&[]byte("test")[0]),
					formatFlags: 0x55,
				},
			},
		},
		{
			name: "Test copying non-NULL tuple with medium length",
			src: Tuple{
				data:        unsafe.Pointer(&[]byte("test123456")[0]),
				formatFlags: 0xFF,
			},
			lenFunc: func(desc *BTDesc, tuple Tuple, lenType LengthType) int {
				return 10
			},
			expected: FixedKey{
				tuple: Tuple{
					data:        unsafe.Pointer(&[]byte("test123456")[0]),
					formatFlags: 0xFF,
				},
			},
		},
		{
			name: "Test copying non-NULL tuple with maximum length",
			src: Tuple{
				data:        unsafe.Pointer(&[]byte("test123456789012")[0]),
				formatFlags: 0xAA,
			},
			lenFunc: func(desc *BTDesc, tuple Tuple, lenType LengthType) int {
				return 15
			},
			expected: FixedKey{
				tuple: Tuple{
					data:        unsafe.Pointer(&[]byte("test123456789012")[0]),
					formatFlags: 0xAA,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock BTreeOps
			mockOps := &MockBTreeOps{
				lenFunc: tt.lenFunc,
				tupleMakeKeyFunc: func(desc *BTDesc, tuple Tuple, data unsafe.Pointer, keepVersion bool, allocated *bool) Tuple {
					return tuple
				},
			}

			// Create BTDesc with mock ops
			desc := &BTDesc{
				ops: mockOps,
			}

			// Create destination key
			var dst FixedKey

			// Copy the source tuple to the destination key
			copyFixedKey(desc, &dst, tt.src)

			// For NULL tuple, verify the key is cleared
			if TupleIsNULL(tt.src) {
				if dst.tuple.data != nil {
					t.Errorf("copyFixedKey() data = %v, want nil for NULL source", dst.tuple.data)
				}
				if dst.tuple.formatFlags != 0 {
					t.Errorf("copyFixedKey() formatFlags = %v, want 0 for NULL source", dst.tuple.formatFlags)
				}
				return
			}

			// For non-NULL tuple, verify the copy
			if dst.tuple.formatFlags != tt.src.formatFlags {
				t.Errorf("copyFixedKey() formatFlags = %v, want %v", dst.tuple.formatFlags, tt.src.formatFlags)
			}

			// Verify the data was copied correctly
			if dst.tuple.data == nil {
				t.Error("copyFixedKey() data is nil for non-NULL source")
			} else {
				// Get the expected length from the mock function
				expectedLen := tt.lenFunc(desc, tt.src, KeyLength)

				// Compare the copied data with the source data
				srcData := (*[1 << 30]byte)(tt.src.data)[:expectedLen:expectedLen]
				dstData := (*[1 << 30]byte)(dst.tuple.data)[:expectedLen:expectedLen]
				for i := 0; i < expectedLen; i++ {
					if srcData[i] != dstData[i] {
						t.Errorf("copyFixedKey() data[%d] = %v, want %v", i, dstData[i], srcData[i])
					}
				}

				// Verify padding bytes are zeroed
				alignedLen := util.AlignValue(expectedLen, 8)
				if expectedLen != alignedLen {
					dstData = (*[1 << 30]byte)(dst.tuple.data)[expectedLen:alignedLen]
					for i := 0; i < len(dstData); i++ {
						if dstData[i] != 0 {
							t.Errorf("copyFixedKey() padding[%d] = %v, want 0", i, dstData[i])
						}
					}
				}
			}
		})
	}
}
