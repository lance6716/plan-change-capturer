package compare

import (
	"github.com/lance6716/plan-change-capturer/pkg/plan"
	"github.com/pingcap/tidb/pkg/util/plancodec"
)

type Result string

const (
	Same Result = "same"
	Diff        = "different"
)

// CmpPlan compares two plan trees and returns the result. Please note that the
// input will be modified in-place.
func CmpPlan(a, b *plan.Op) Result {
	// projection will not affect the performance, so we remove it before comparing.
	removeProj(a)
	removeProj(b)
	return cmpPlan(a, b)
}

func cmpPlan(a, b *plan.Op) Result {
	if a.Type != b.Type {
		return Diff
	}
	if len(a.Children) != len(b.Children) {
		return Diff
	}
	for i := range a.Children {
		if r := cmpPlan(a.Children[i], b.Children[i]); r != Same {
			return r
		}
	}
	return Same
}

// removeProj removes the Projection operator from the plan tree in-place.
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
