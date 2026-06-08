// Package errutil contains helper functions for handling errors
package errutil

import (
	"errors"

	"github.com/lib/pq"
)

// IsUniqueViolation checks if the given error is a PostgreSQL unique constraint violation (Code 23505).
func IsUniqueViolation(err error) bool {
	if err == nil {
		return false
	}

	if pqErr, ok := errors.AsType[*pq.Error](err); ok {
		return pqErr.Code == "23505"
	}

	return false
}
