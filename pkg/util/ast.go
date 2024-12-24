package util

import "github.com/pingcap/tidb/pkg/parser/ast"

type visitor struct {
	currDB     string
	tableNames [][2]string
}

func (v *visitor) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch n := in.(type) {
	case *ast.TableName:
		if n.Schema.L == "" {
			n.Schema.L = v.currDB
		}
		v.tableNames = append(v.tableNames, [2]string{n.Schema.L, n.Name.L})
	}
	return in, false
}

func (v *visitor) Leave(in ast.Node) (out ast.Node, ok bool) {
	return in, true
}

// ExtractTableNames extracts all table names from a statement node.
func ExtractTableNames(s ast.StmtNode, currDB string) [][2]string {
	v := &visitor{currDB: currDB}
	s.Accept(v)
	return v.tableNames
}
