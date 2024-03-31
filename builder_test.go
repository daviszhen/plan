package main

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func toJson(root any, path string) error {
	data, err := json.Marshal(root)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0755)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.Write(data)
	if err != nil {
		return err
	}
	return nil
}

func runTest(t *testing.T, name string, ast *Ast) {
	builder := NewBuilder()
	err := builder.buildSelect(ast, builder.rootCtx, 0)
	assert.NoError(t, err)

	// fmt.Println(builder.String())
	assert.Greater(t, *builder.tag, 0)

	lp, err := builder.CreatePlan(builder.rootCtx, nil)
	assert.NoError(t, err)
	assert.NotNil(t, lp)
	fmt.Println("Before Optimize\n", lp.String())
	lp, err = builder.Optimize(builder.rootCtx, lp)
	assert.NoError(t, err)
	assert.NotNil(t, lp)
	fmt.Println("After Optimize\n", lp.String())

	err = toJson(lp, "./testout/"+name)
	assert.NoError(t, err)

	pp, err := builder.CreatePhyPlan(lp)
	assert.NoError(t, err)
	assert.NotNil(t, pp)
	fmt.Println("\n", pp.String())

	err = toJson(pp, "./testout/p"+name)
	assert.NoError(t, err)

}

func TestQ1(t *testing.T) {
	q1 := tpchQ1()
	runTest(t, "q1.json", q1)
}

func TestQ2(t *testing.T) {
	q2 := tpchQ2()
	runTest(t, "q2.json", q2)
}

func TestQ3(t *testing.T) {
	q3 := tpchQ3()
	runTest(t, "q3.json", q3)
}

func TestQ4(t *testing.T) {
	q4 := tpchQ4()
	runTest(t, "q4.json", q4)
}

func TestQ5(t *testing.T) {
	q5 := tpchQ5()
	runTest(t, "q5.json", q5)
}

func TestQ6(t *testing.T) {
	q6 := tpchQ6()
	runTest(t, "q6.json", q6)
}

func TestQ7(t *testing.T) {
	q7 := tpchQ7()
	runTest(t, "q7.json", q7)
}

func TestQ8(t *testing.T) {
	q8 := tpchQ8()
	runTest(t, "q8.json", q8)
}

func TestQ9(t *testing.T) {
	q9 := tpchQ9()
	runTest(t, "q9.json", q9)
}

func TestQ10(t *testing.T) {
	q10 := tpchQ10()
	runTest(t, "q10.json", q10)
}

func TestQ11(t *testing.T) {
	/*
		TODO: fix
			└── Order:
			    ├── [exprs]
			    │     0: (.,<invalid,null,0,0>,[1,1],0) desc
	*/
	q11 := tpchQ11()
	runTest(t, "q11.json", q11)
}

func TestQ12(t *testing.T) {
	q12 := tpchQ12()
	runTest(t, "q12.json", q12)
}

func TestQ13(t *testing.T) {
	/*
		TODO:fix
			join order on the left join
	*/
	q13 := tpchQ13()
	runTest(t, "q13.json", q13)
}

func TestQ14(t *testing.T) {
	q14 := tpchQ14()
	runTest(t, "q14.json", q14)
}

func TestQ15(t *testing.T) {
	q15 := tpchQ15()
	runTest(t, "q15.json", q15)
}

func TestQ16(t *testing.T) {
	q16 := tpchQ16()
	runTest(t, "q16.json", q16)
}

func TestQ17(t *testing.T) {
	q17 := tpchQ17()
	runTest(t, "q17.json", q17)
}

func TestQ18(t *testing.T) {
	q18 := tpchQ18()
	runTest(t, "q18.json", q18)
}

func TestQ19(t *testing.T) {
	q19 := tpchQ19()
	runTest(t, "q19.json", q19)
}

func TestQ20(t *testing.T) {
	q20 := tpchQ20()
	runTest(t, "q20.json", q20)
}

func TestQ21(t *testing.T) {
	q21 := tpchQ21()
	runTest(t, "q21.json", q21)
}

func TestQ22(t *testing.T) {
	q22 := tpchQ22()
	runTest(t, "q22.json", q22)
}
