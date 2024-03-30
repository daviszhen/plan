package main

type BooleanOp interface {
	opWithoutNull(bool, bool) bool
	opWithNull(bool, bool, bool, bool) (bool, bool)
}

var gAndOp AndOp

type AndOp struct {
}

/*
TRUE  AND TRUE   = TRUE

TRUE  AND FALSE  = FALSE
FALSE AND TRUE   = FALSE
FALSE AND FALSE  = FALSE
*/
func (add AndOp) opWithoutNull(left, right bool) bool {
	return left && right
}

/*
true: both are true
false: either is false. neglect NULL
NULL: otherwise

TRUE  AND TRUE   = TRUE

TRUE  AND FALSE  = FALSE
FALSE AND TRUE   = FALSE
FALSE AND FALSE  = FALSE

FALSE AND NULL   = FALSE
NULL  AND FALSE  = FALSE

TRUE  AND NULL   = NULL
NULL  AND TRUE   = NULL
NULL  AND NULL   = NULL
*/
func (add AndOp) opWithNull(left, right, lnull, rnull bool) (null bool, result bool) {
	if lnull && rnull {
		//NULL  AND NULL   = NULL
		return true, true
	} else if lnull {
		//NULL  AND FALSE  = FALSE
		//NULL  AND TRUE   = NULL
		//NULL  AND NULL   = NULL
		return right, right
	} else if rnull {
		//FALSE AND NULL   = FALSE
		//TRUE  AND NULL   = NULL
		//NULL  AND NULL   = NULL
		return left, left
	} else {
		//TRUE  AND TRUE   = TRUE
		//
		//TRUE  AND FALSE  = FALSE
		//FALSE AND TRUE   = FALSE
		//FALSE AND FALSE  = FALSE
		return false, left && right
	}
}
