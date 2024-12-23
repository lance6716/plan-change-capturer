package plan

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/util/texttree"
)

// newPlanFromSQLResultRow parses the result from SQL query into an Op tree.
//
// The input is a slice of [id, task, access object] fields or [id, task,
// operator info] fields.
func newPlanFromSQLResultRow(result [][3]string) (*Op, error) {
	// TODO(lance6716): check every error, attach enough information to them
	if len(result) == 0 {
		return nil, fmt.Errorf("input has zero length")
	}

	stack := make([]*Op, 0, len(result)/2)

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

		newOp, err := NewOp(fullName, taskCol, accessObjCol)
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

func NewPlanFromStmtSummaryPlan(planStr string) (*Op, error) {
	lines := strings.Split(planStr, "\n")
	if len(lines) < 2 {
		// there should be at least a header and a plan line
		return nil, errors.Errorf("invalid plan string: %s", planStr)
	}
	columnNames := strings.Split(lines[0], "\t")
	for i := range columnNames {
		columnNames[i] = strings.TrimRight(columnNames[i], " ")
	}
	idColIdx := slices.Index(columnNames, "id")
	if idColIdx == -1 {
		return nil, errors.Errorf("column `id` not found in the header: %s", lines[0])
	}
	taskColIdx := slices.Index(columnNames, "task")
	if taskColIdx == -1 {
		return nil, errors.Errorf("column `task` not found in the header: %s", lines[0])
	}
	opInfoColIdx := slices.Index(columnNames, "operator info")
	if opInfoColIdx == -1 {
		return nil, errors.Errorf("column `operator info` not found in the header: %s", lines[0])
	}

	result := make([][3]string, 0, len(lines)-1)
	for i := 1; i < len(lines); i++ {
		fields := strings.Split(lines[i], "\t")
		if len(fields) != len(columnNames) {
			return nil, errors.Errorf(
				"column count mismatch at line %d\nfirst line: %s\nmismatch line: %s",
				i, lines[0], lines[i],
			)
		}
		result = append(result, [3]string{
			strings.TrimRight(fields[idColIdx], " "),
			strings.TrimRight(fields[taskColIdx], " "),
			strings.TrimRight(fields[opInfoColIdx], " "),
		})
	}

	return newPlanFromSQLResultRow(result)
}

func NewPlanFromQuery(
	ctx context.Context,
	db *sql.DB,
	dbName string,
	query string,
) (*Op, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, errors.Annotatef(err, "failed to get connection for database: %s, query: %s", dbName, query)
	}
	defer conn.Close()

	_, err = conn.ExecContext(ctx, "USE "+dbName)
	if err != nil {
		return nil, errors.Annotatef(err, "failed to execute USE for database: %s, query: %s", dbName, query)
	}

	rows, err := conn.QueryContext(ctx, "EXPLAIN "+query)
	if err != nil {
		return nil, errors.Annotatef(err, "failed to execute EXPLAIN for database: %s, query: %s", dbName, query)
	}
	defer rows.Close()

	columnNames, err := rows.Columns()
	if err != nil {
		return nil, errors.Annotatef(err, "failed to get columns for database: %s, query: %s", dbName, query)
	}
	idColIdx := slices.Index(columnNames, "id")
	if idColIdx == -1 {
		return nil, errors.Errorf("column `id` not found in the result. columns: %v", columnNames)
	}
	taskColIdx := slices.Index(columnNames, "task")
	if taskColIdx == -1 {
		return nil, errors.Errorf("column `task` not found in the header. columns: %v", columnNames)
	}
	accessObjColIdx := slices.Index(columnNames, "access object")
	if accessObjColIdx == -1 {
		return nil, errors.Errorf("column `access object` not found in the header. columns: %v", columnNames)
	}

	result := make([][3]string, 0)
	scanDest := make([]any, len(columnNames))
	for i := range scanDest {
		scanDest[i] = new(any)
	}
	var idCol, taskCol, accessObjCol string
	scanDest[idColIdx] = &idCol
	scanDest[taskColIdx] = &taskCol
	scanDest[accessObjColIdx] = &accessObjCol
	for rows.Next() {
		err = rows.Scan(scanDest...)
		if err != nil {
			return nil, errors.Annotatef(err, "failed to scan row for database: %s, query: %s", dbName, query)
		}
		result = append(result, [3]string{idCol, taskCol, accessObjCol})
	}
	if err = rows.Close(); err != nil {
		return nil, errors.Annotatef(err, "failed to iterate rows for database: %s, query: %s", dbName, query)
	}

	return newPlanFromSQLResultRow(result)
}
