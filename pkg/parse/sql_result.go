package parse

import (
	"fmt"

	"github.com/lance6716/plan-change-capturer/pkg/plan"
	"github.com/pingcap/tidb/pkg/util/texttree"
)

// NewPlanFromSQLResultRow parses the result from EXPLAIN FORMAT = 'row' ...
// statement into an Op tree.
//
// The input is a slice of [id, task, access object] fields.
func NewPlanFromSQLResultRow(result [][3]string) (*plan.Op, error) {
	if len(result) == 0 {
		return nil, fmt.Errorf("input has zero length")
	}

	stack := make([]*plan.Op, 0, len(result)/2)

	for _, fields := range result {
		idCol, taskCol, accessObjCol := fields[0], fields[1], fields[2]
		if idCol == "" {
			return nil, fmt.Errorf("`id` column is empty")
		}
		// iterate over runes. Runes are not always single byte.
		runes := []rune(idCol)

		indentLen := 0
		for _, r := range runes {
			switch r {
			case texttree.TreeBody, texttree.TreeMiddleNode,
				texttree.TreeLastNode, texttree.TreeGap,
				texttree.TreeNodeIdentifier:
				indentLen++
			default:
				break
			}
		}
		if indentLen%2 != 0 {
			return nil, fmt.Errorf(
				"the indent is not expected, its length should be a multiple of 2: %s",
				idCol,
			)
		}

		identLevel := indentLen / 2
		if identLevel > len(stack) {
			return nil, fmt.Errorf(
				"the indent level (%d) is larger than the stack size (%d): %s",
				identLevel, len(stack), idCol,
			)
		}
		stack = stack[:identLevel]
		fullName := string(runes[indentLen:])

		newOp, err := plan.NewOp(fullName, taskCol, accessObjCol)
		if err != nil {
			return nil, err
		}
		if len(stack) > 0 {
			stack[len(stack)-1].Children = append(stack[len(stack)-1].Children, newOp)
		}
		stack = append(stack, newOp)
	}

	return stack[0], nil
}
