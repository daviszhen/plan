package main

func (b *Builder) joinOrder(root *LogicalOperator) (*LogicalOperator, error) {
	var err error

	//1. construct graphs
	//2. split into multiple connected-sub-graph
	//3. decide join order for every connected-sub-graph
	//3.1 baseline algo1: DPSize
	//3.2 baseline algo2 : DPSub
	//3.3 advanced algo : DPhyp
	//4. composite the complete join

	return root, err
}
