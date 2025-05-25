package chunk

import "fmt"

type PhyFormat int

const (
	PF_FLAT PhyFormat = iota
	PF_CONST
	PF_DICT
	PF_SEQUENCE
)

func (f PhyFormat) String() string {
	switch f {
	case PF_FLAT:
		return "flat"
	case PF_CONST:
		return "constant"
	case PF_DICT:
		return "dictionary"
	}
	panic(fmt.Sprintf("usp %d", f))
}

func (f PhyFormat) IsConst() bool {
	return f == PF_CONST
}

func (f PhyFormat) IsFlat() bool {
	return f == PF_FLAT
}

func (f PhyFormat) IsDict() bool {
	return f == PF_DICT
}

func (f PhyFormat) IsSequence() bool {
	return f == PF_SEQUENCE
}
