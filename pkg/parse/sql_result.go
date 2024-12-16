package parse

import (
	"fmt"

	"github.com/lance6716/plan-change-capturer/pkg/plan"
	"github.com/pingcap/tidb/pkg/util/texttree"
)

// ParseSQLResultRow parses the result from EXPLAIN FORMAT = 'row' ... statement
// into an Op tree.
func ParseSQLResultRow(idCols []string) (*plan.Op, error) {
	if len(idCols) == 0 {
		return nil, fmt.Errorf("no content in id column")
	}

	stack := make([]*plan.Op, 0, len(idCols)/2)

	for _, idCol := range idCols {
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
		newOp := &plan.Op{FullName: fullName}
		if len(stack) > 0 {
			stack[len(stack)-1].Children = append(stack[len(stack)-1].Children, newOp)
		}
		stack = append(stack, newOp)
	}

	return stack[0], nil
}
