package compute

import (
	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/storage"
)

func collectTags(root *LogicalOperator, set map[uint64]bool) {
	if root.Index != 0 {
		set[root.Index] = true
	}
	if root.Index2 != 0 {
		set[root.Index2] = true
	}
	for _, child := range root.Children {
		collectTags(child, set)
	}
}

const (
	NoneSide       = 0
	LeftSide       = 1 << 1
	RightSide      = 1 << 2
	BothSide       = LeftSide | RightSide
	CorrelatedSide = 1 << 3
)

//////////////////////////////////////////////
// create physical plan
//////////////////////////////////////////////

func (b *Builder) CreatePhyPlan(root *LogicalOperator) (*PhysicalOperator, error) {
	if root == nil {
		return nil, nil
	}
	var err error
	children := make([]*PhysicalOperator, 0)
	for _, child := range root.Children {
		childPlan, err := b.CreatePhyPlan(child)
		if err != nil {
			return nil, err
		}
		children = append(children, childPlan)
	}
	var proot *PhysicalOperator
	switch root.Typ {
	case LOT_Project:
		proot, err = b.createPhyProject(root, children)
		if err != nil {
			return nil, err
		}
	case LOT_Filter:
		proot, err = b.createPhyFilter(root, children)
		if err != nil {
			return nil, err
		}
	case LOT_Scan:
		proot, err = b.createPhyScan(root, children)
		if err != nil {
			return nil, err
		}
	case LOT_JOIN:
		proot, err = b.createPhyJoin(root, children)
		if err != nil {
			return nil, err
		}
	case LOT_AggGroup:
		proot, err = b.createPhyAgg(root, children)
		if err != nil {
			return nil, err
		}
	case LOT_Order:
		proot, err = b.createPhyOrder(root, children)
		if err != nil {
			return nil, err
		}
	case LOT_Limit:
		proot, err = b.createPhyLimit(root, children)
		if err != nil {
			return nil, err
		}
	case LOT_CreateSchema:
		proot, err = b.createPhyCreateSchema(root, children)
		if err != nil {
			return nil, err
		}
	case LOT_CreateTable:
		proot, err = b.createPhyCreateTable(root, children)
		if err != nil {
			return nil, err
		}
	case LOT_Insert:
		proot, err = b.createPhyInsert(root, children)
		if err != nil {
			return nil, err
		}
	default:
		panic("usp")
	}
	if proot != nil {
		proot.estimatedCard = root.estimatedCard
	}

	proot.Id = b.getPhyId()
	return proot, nil
}

func (b *Builder) createPhyProject(root *LogicalOperator, children []*PhysicalOperator) (*PhysicalOperator, error) {
	return &PhysicalOperator{
		Typ:      POT_Project,
		Index:    root.Index,
		Projects: root.Projects,
		Outputs:  root.Outputs,
		Children: children}, nil
}

func (b *Builder) createPhyFilter(root *LogicalOperator, children []*PhysicalOperator) (*PhysicalOperator, error) {
	return &PhysicalOperator{Typ: POT_Filter, Filters: root.Filters, Outputs: root.Outputs, Children: children}, nil
}

func (b *Builder) createPhyScan(root *LogicalOperator, children []*PhysicalOperator) (*PhysicalOperator, error) {
	ret := &PhysicalOperator{
		Typ:         POT_Scan,
		Index:       root.Index,
		Database:    root.Database,
		Table:       root.Table,
		Alias:       root.Alias,
		Outputs:     root.Outputs,
		Columns:     root.Columns,
		Filters:     root.Filters,
		ScanTyp:     root.ScanTyp,
		Types:       root.Types,
		ColName2Idx: root.ColName2Idx,
		Children:    children}

	switch root.ScanTyp {
	case ScanTypeValuesList:
		{
			var valuesExec *ExprExec
			var err error

			collection := NewColumnDataCollection(root.Types)
			data := &chunk.Chunk{}
			data.Init(root.Types, storage.STANDARD_VECTOR_SIZE)

			tmp := &chunk.Chunk{}
			tmp.SetCard(1)

			for i := 0; i < len(root.Values); i++ {
				valuesExec = NewExprExec(root.Values[i]...)
				err = valuesExec.executeExprs(
					[]*chunk.Chunk{tmp, nil, nil},
					data)
				if err != nil {
					return nil, err
				}
				collection.Append(data)
				data.Reset()
			}
			ret.collection = collection
		}
	case ScanTypeTable:
	case ScanTypeCopyFrom:
		ret.ScanInfo = root.ScanInfo
	}

	return ret, nil
}

func (b *Builder) createPhyJoin(root *LogicalOperator, children []*PhysicalOperator) (*PhysicalOperator, error) {
	return &PhysicalOperator{
		Typ:      POT_Join,
		Index:    root.Index,
		JoinTyp:  root.JoinTyp,
		OnConds:  root.OnConds,
		Outputs:  root.Outputs,
		Children: children}, nil
}

func (b *Builder) createPhyOrder(root *LogicalOperator, children []*PhysicalOperator) (*PhysicalOperator, error) {
	return &PhysicalOperator{
		Typ:      POT_Order,
		OrderBys: root.OrderBys,
		Outputs:  root.Outputs,
		Children: children}, nil
}

func (b *Builder) createPhyAgg(root *LogicalOperator, children []*PhysicalOperator) (*PhysicalOperator, error) {
	return &PhysicalOperator{
		Typ:      POT_Agg,
		Index:    root.Index,
		Index2:   root.Index2,
		Filters:  root.Filters,
		Aggs:     root.Aggs,
		GroupBys: root.GroupBys,
		Outputs:  root.Outputs,
		Children: children}, nil
}

func (b *Builder) createPhyLimit(root *LogicalOperator, children []*PhysicalOperator) (*PhysicalOperator, error) {
	return &PhysicalOperator{
		Typ:      POT_Limit,
		Outputs:  root.Outputs,
		Limit:    root.Limit,
		Offset:   root.Offset,
		Children: children}, nil
}
