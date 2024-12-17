package plan

import (
	"fmt"
	"strings"
)

type Op struct {
	// FullName has the format of "{Type}_{ID}{Label}".
	FullName string
	Type     string
	ID       string
	Label    string

	Children []*Op
}

// NewOp creates a new Op from given full name.
func NewOp(fullName string) (*Op, error) {
	ret := &Op{FullName: fullName}
	i := strings.IndexByte(fullName, '_')
	if i == -1 {
		return nil, fmt.Errorf("invalid plan opeartor: %s", fullName)
	}
	ret.Type = fullName[:i]
	j := strings.LastIndexAny(fullName, "0123456789")
	if j == -1 {
		return nil, fmt.Errorf("invalid plan opeartor: %s", fullName)
	}
	ret.ID = fullName[i+1 : j+1]
	ret.Label = fullName[j+1:]
	return ret, nil
}

// MustNewOp panic if NewOp failed.
func MustNewOp(fullName string) *Op {
	op, err := NewOp(fullName)
	if err != nil {
		panic(err)
	}
	return op
}
