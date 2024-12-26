package compare

import "github.com/lance6716/plan-change-capturer/pkg/source"

// PlanCmpResult is the result of comparing two plans, which is the final result
// unit of pcc. The ID of PlanCmpResult is the same as the ID of the
// source.StmtSummary, which is OldVersionInfo.SQLDigest +
// OldVersionInfo.PlanDigest.
type PlanCmpResult struct {
	ErrMsg string
	Result Result

	OldVersionInfo *source.StmtSummary
	OldPlan        string
	NewDiffPlan    string
}

// TODO(lance6716): support execution comparison.
type ExecCmpResult struct {
}
