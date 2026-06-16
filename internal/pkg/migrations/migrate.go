package migrations

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"

	"github.com/i-christian/pgqueue/internal/pkg/queries"
)

//go:embed schema.sql
var SchemaSQL string

// Migrate applies the base schema and ensures required partitions exist.
func Migrate(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, SchemaSQL); err != nil {
		return fmt.Errorf("schema execution failed: %w", err)
	}

	_, err := db.ExecContext(ctx, queries.EnsurePartitions)

	return err
}
