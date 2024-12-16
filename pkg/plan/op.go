package plan

type Op struct {
	FullName string

	Children []*Op
}
