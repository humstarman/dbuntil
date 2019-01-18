package dbutil

import (
	"github.com/pkg/errors"
	"strings"
)

const (
	Sep = ","
)

func PlusNonNull(x string, y string) string {
	if y == "" {
		return x
	}
	if x == "" {
		return y
	}
	return fmt.Sprintf("%v%v%v", x, Sep, y)
}

func AssignValue(v []string, idx int) string {
	if idx < len(v) {
		return v[idx]
	}
	return ""
}

func FromTableToString(t *Table) (string, error) {
	v := ""
	v = PlusNonNull(v, t.V0)
	v = PlusNonNull(v, t.V1)
	return v, nil
}

func FromStringToTable(str) (*Table, error) {
	if str == "" {
		err := errors.New("empty strings")
		return nil, err
	}
	t := Table{}
	v := strings.Split(str, Sep)
	t.V0 = AssignValue(v, 0)
	t.V1 = AssignValue(v, 1)
	return &t, nill
}
