package main

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
func andWithNull(left, right, lnull, rnull bool) (null bool, result bool) {
	return false, false
}
