package expression

type InWhichClause int

const (
	IWC_SELECT InWhichClause = iota
	IWC_WHERE
	IWC_GROUP
	IWC_HAVING
	IWC_ORDER
	IWC_LIMIT
	IWC_JOINON
	IWC_VALUES //for insert ... values
)
