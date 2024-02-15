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
	assert.NotNil(t, builder.whereExpr)

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
