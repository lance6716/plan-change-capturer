package util

import (
	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/errno"
)

type unretryableErr interface {
	marker()
}

type unretryableWrapper struct {
	error
}

func (unretryableWrapper) marker() {}

// WrapUnretryableError wraps an error to make it unretryable.
func WrapUnretryableError(err error) error {
	return unretryableWrapper{err}
}

// IsUnretryableError checks if an error is wrapped by WrapUnretryableError. It
// supports pingcap/errors package.
func IsUnretryableError(err error) bool {
	for err != nil {
		if _, ok := err.(unretryableErr); ok {
			return true
		}
		err = errors.Unwrap(err)
	}
	return false
}

// CheckOldDBSQLErrorUnretryable checks the MySQL error from old version database
// to determine if it is unretryable. For errors we don't have confidence, we
// assume it is retryable so caller will execute the statement again in future.
func CheckOldDBSQLErrorUnretryable(err *mysql.MySQLError) bool {
	if err == nil {
		return false
	}
	switch err.Number {
	case errno.ErrParse, errno.ErrNoSuchTable, errno.ErrBadDB:
		return true
	}
	return false
}
