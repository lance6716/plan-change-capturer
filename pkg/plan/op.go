package plan

import (
	"encoding/json"
	"fmt"
	"strings"
)

type AccessObject struct {
	Table      string
	Index      string
	Partitions []string
	CTE        string

	// TODO(lance6716): learn the meaning of this field.
	DynamicPartitionRawStr string
}

type Op struct {
	Type  string
	ID    string
	Label string

	Task string

	AccessObject *AccessObject

	Children []*Op
}

// NewOp creates a new Op from given full name.
func NewOp(fullName, task, accessObjectStr string) (*Op, error) {
	ret := &Op{}
	// FullName has the format of "{Type}_{ID}{Label}".
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

	ret.Task = task
	if accessObjectStr != "" {
		accessObject, err := parseAccessObject(accessObjectStr)
		if err != nil {
			return nil, err
		}
		ret.AccessObject = accessObject
	}
	return ret, nil
}

func parseAccessObject(str string) (*AccessObject, error) {
	// see tidb's AccessObject.String()
	ret := &AccessObject{}

	// just hope no special characters in SQL identifiers
	const (
		tablePrefix     = "table:"
		objectSep       = ", "
		partitionPrefix = "partition:"
		indexPrefix     = "index:"
	)
	if strings.HasPrefix(str, tablePrefix) {
		str = str[len(tablePrefix):]
		i := strings.Index(str, objectSep)
		if i == -1 {
			// table:t1
			ret.Table = str
			return ret, nil
		}
		ret.Table = str[:i]
		str = str[i+len(objectSep):]

		if strings.HasPrefix(str, partitionPrefix) {
			str = str[len(partitionPrefix):]
			i = strings.Index(str, objectSep)
			if i == -1 {
				// table:t1, partition:p0,p1,p2
				ret.Partitions = strings.Split(str, ",")
				return ret, nil
			}
			ret.Partitions = strings.Split(str[:i], ",")
			str = str[i+len(objectSep):]
		}

		// table:t4, index:idx(a, b)
		if strings.HasPrefix(str, indexPrefix) {
			ret.Index = str[len(indexPrefix):]
		}
		return ret, nil
	}

	panic("not implemented")
}

// NewOp4Test creates a Op for test. The input string should be in the format of
// - {fullName}, where fullName should not contain "|"
// - {fullName}|{accessObjectStr}
func NewOp4Test(input string) *Op {
	var fullName, accessObjectStr string
	parts := strings.Split(input, "|")
	switch len(parts) {
	case 1:
		fullName = parts[0]
	case 2:
		fullName = parts[0]
		accessObjectStr = parts[1]
	}

	op, err := NewOp(fullName, "test", accessObjectStr)
	if err != nil {
		panic(err)
	}
	return op
}

func (o *Op) Clone() *Op {
	bs, _ := json.Marshal(o)
	ret := &Op{}
	_ = json.Unmarshal(bs, ret)
	return ret
}
