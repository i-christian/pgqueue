package main

import (
	"time"

	"github.com/google/uuid"
)

// extractTimeFromUUIDv7 extracts timestamp from UUIDv7 to allow Postgres Partition Pruning.
func extractTimeFromUUIDv7(idStr string) (time.Time, error) {
	id, err := uuid.Parse(idStr)
	if err != nil {
		return time.Time{}, err
	}

	ms := int64(id[0])<<40 | int64(id[1])<<32 | int64(id[2])<<24 |
		int64(id[3])<<16 | int64(id[4])<<8 | int64(id[5])

	return time.UnixMilli(ms).UTC(), nil
}
