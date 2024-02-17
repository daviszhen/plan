package main

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func toJson(root *LogicalOperator, path string) error {
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
	assert.Greater(t, builder.tag, 0)

	lp, err := builder.CreatePlan(builder.rootCtx, nil)
	assert.NoError(t, err)
	assert.NotNil(t, lp)
	fmt.Println("Before Optimize\n", lp.String())
	lp, err = builder.Optimize(builder.rootCtx, lp)
	assert.NoError(t, err)
	assert.NotNil(t, lp)
	fmt.Println("After Optimize\n", lp.String())

	err = toJson(lp, "./"+name)
	assert.NoError(t, err)
}

func TestQ2(t *testing.T) {
	q2 := tpchQ2()
	runTest(t, "q2.json", q2)
}

func TestQ4(t *testing.T) {
	q4 := tpchQ4()
	runTest(t, "q4.json", q4)
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

func TestQ11(t *testing.T) {
	q11 := tpchQ11()
	runTest(t, "q11.json", q11)
}