package stringutils

import (
	"time"

	"github.com/google/uuid"
)

// NewUUIDv7 generates a new UUIDv7 and returns the ID with its internal UTC timestamp.
func NewUUIDv7() (uuid.UUID, time.Time, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return uuid.Nil, time.Time{}, err
	}
	return id, ExtractTimeFromUUIDv7(id), nil
}

// ExtractTimeFromUUIDv7 decodes the 48-bit UNIX timestamp and forces UTC.
func ExtractTimeFromUUIDv7(id uuid.UUID) time.Time {
	ms := int64(id[0])<<40 | int64(id[1])<<32 | int64(id[2])<<24 |
		int64(id[3])<<16 | int64(id[4])<<8 | int64(id[5])
	return time.UnixMilli(ms).UTC()
}
