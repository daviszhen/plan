package compute

import (
	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

type HashJoinStage int

const (
	HJS_INIT HashJoinStage = iota
	HJS_BUILD
	HJS_PROBE
	HJS_SCAN_HT
	HJS_DONE
)

type HashJoin struct {
	_conds []*Expr

	//types of the keys. the type of left part in the join on condition expr.
	_keyTypes []common.LType

	//types of right children of join.
	_buildTypes []common.LType

	_ht *JoinHashTable

	//for hash join
	_buildExec *ExprExec

	_joinKeys *chunk.Chunk

	_buildChunk *chunk.Chunk

	_hjs      HashJoinStage
	_scan     *Scan
	_probExec *ExprExec
	//types of the output Chunk in Scan.Next
	_scanNextTyps []common.LType

	//colIdx of the left(right) Children in the output Chunk in Scan.Next
	_leftIndice  []int
	_rightIndice []int
	_markIndex   int
}

func NewHashJoin(op *PhysicalOperator, conds []*Expr) *HashJoin {
	hj := new(HashJoin)
	hj._hjs = HJS_INIT
	hj._conds = copyExprs(conds...)
	for _, cond := range conds {
		hj._keyTypes = append(hj._keyTypes, cond.Children[0].DataTyp)
	}

	for i, output := range op.Children[0].Outputs {
		hj._scanNextTyps = append(hj._scanNextTyps, output.DataTyp)
		hj._leftIndice = append(hj._leftIndice, i)
	}

	if op.JoinTyp != LOT_JoinTypeSEMI && op.JoinTyp != LOT_JoinTypeANTI {
		//right child output types
		rightIdxOffset := len(hj._scanNextTyps)
		for i, output := range op.Children[1].Outputs {
			hj._buildTypes = append(hj._buildTypes, output.DataTyp)
			hj._scanNextTyps = append(hj._scanNextTyps, output.DataTyp)
			hj._rightIndice = append(hj._rightIndice, rightIdxOffset+i)
		}
	}

	if op.JoinTyp == LOT_JoinTypeMARK || op.JoinTyp == LOT_JoinTypeAntiMARK {
		hj._scanNextTyps = append(hj._scanNextTyps, common.BooleanType())
		hj._markIndex = len(hj._scanNextTyps) - 1
	}
	//
	hj._buildChunk = &chunk.Chunk{}
	hj._buildChunk.Init(hj._buildTypes, util.DefaultVectorSize)

	//
	hj._buildExec = &ExprExec{}
	for _, cond := range hj._conds {
		//FIXME: Children[1] may not be from right part
		//FIX it in the build stage.
		hj._buildExec.addExpr(cond.Children[1])
	}

	hj._joinKeys = &chunk.Chunk{}
	hj._joinKeys.Init(hj._keyTypes, util.DefaultVectorSize)

	hj._ht = NewJoinHashTable(conds, hj._buildTypes, op.JoinTyp)

	hj._probExec = &ExprExec{}
	for _, cond := range hj._conds {
		hj._probExec.addExpr(cond.Children[0])
	}
	return hj
}

func (hj *HashJoin) Build(input *chunk.Chunk) error {
	var err error
	//evaluate the right part
	hj._joinKeys.Reset()
	err = hj._buildExec.executeExprs([]*chunk.Chunk{nil, input, nil},
		hj._joinKeys)
	if err != nil {
		return err
	}

	//build th
	if len(hj._buildTypes) != 0 {
		hj._ht.Build(hj._joinKeys, input)
	} else {
		hj._buildChunk.SetCard(input.Card())
		hj._ht.Build(hj._joinKeys, hj._buildChunk)
	}
	return nil
}
