package compare

import (
	"github.com/lance6716/plan-change-capturer/pkg/plan"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/pingcap/tidb/pkg/util/plancodec"
)

type Result string

const (
	Same Result = "same"
	Diff        = "different"
)

// CmpPlan compares two plan trees and returns the result. Please note that the
// input will be modified in-place.
func CmpPlan(sql string, a, b *plan.Op) (Result, error) {
	// projection will not affect the performance, so we remove it before comparing.
	removeProj(a)
	removeProj(b)
	err := normalizeTableNameAlias(sql, a, b)
	if err != nil {
		// TODO(lance6716): we can ignore the error?
		return Diff, err
	}
	return cmpPlan(a, b), nil
}

func cmpPlan(a, b *plan.Op) Result {
	if a.Type != b.Type {
		return Diff
	}
	if len(a.Children) != len(b.Children) {
		return Diff
	}

	if r := cmpAccessObject(a.AccessObject, b.AccessObject); r != Same {
		return r
	}

	for i := range a.Children {
		if r := cmpPlan(a.Children[i], b.Children[i]); r != Same {
			return r
		}
	}
	return Same
}

func cmpAccessObject(a, b *plan.AccessObject) Result {
	if a == nil && b == nil {
		return Same
	}
	if a == nil || b == nil {
		return Diff
	}

	if a.Table != b.Table {
		return Diff
	}
	if a.Index != b.Index {
		return Diff
	}
	if len(a.Partitions) != len(b.Partitions) {
		return Diff
	}
	for i := range a.Partitions {
		if a.Partitions[i] != b.Partitions[i] {
			return Diff
		}
	}
	if a.CTE != b.CTE {
		return Diff
	}
	if a.DynamicPartitionRawStr != b.DynamicPartitionRawStr {
		return Diff
	}
	return Same
}

// removeProj removes the Projection operator from the plan tree in-place.
// Currently we assume all types of projection are negligible to performance.
func removeProj(p *plan.Op) {
	if p.Type == plancodec.TypeProj {
		if len(p.Children) == 1 {
			*p = *p.Children[0]
		}
	}
	for _, child := range p.Children {
		removeProj(child)
	}
}

type aliasVisitor struct {
	alias unionFind
}

func (vis *aliasVisitor) Enter(n ast.Node) (ast.Node, bool) {
	tbl, ok := n.(*ast.TableSource)
	if !ok {
		return n, false
	}

	// TODO(lance6716): should be database.table when consider alias?
	switch v := tbl.Source.(type) {
	case *ast.TableName:
		if tbl.AsName.L != "" {
			vis.alias.union(v.Name.L, tbl.AsName.L)
		}
	}
	return n, false
}

func (vis *aliasVisitor) Leave(n ast.Node) (ast.Node, bool) {
	return n, true
}

func normalizeTableNameAlias(sql string, a *plan.Op, b *plan.Op) error {
	// TODO(lance6716): cache the parser
	p := parser.New()
	// TODO(lance6716): check the result is fetched in default charset and collation
	stmt, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		return err
	}
	v := &aliasVisitor{alias: newUnionFind()}
	stmt.Accept(v)

	rename(v.alias, a)
	rename(v.alias, b)
	return nil
}

func rename(u unionFind, op *plan.Op) {
	if o := op.AccessObject; o != nil {
		o.Table = u.parentOrSelf(o.Table)
	}
	for _, child := range op.Children {
		rename(u, child)
	}
}
