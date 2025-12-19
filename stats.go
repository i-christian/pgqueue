package pgqueue

import "context"

func (c *Client) Stats(ctx context.Context) (QueueStats, error) {
	rows, err := c.db.QueryContext(ctx, `
		SELECT status, count(*) FROM tasks GROUP BY status
	`)
	if err != nil {
		return QueueStats{}, err
	}
	defer rows.Close()

	var s QueueStats
	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			continue
		}
		s.Total += count
		switch status {
		case "pending":
			s.Pending = count
		case "processing":
			s.Processing = count
		case "failed":
			s.Failed = count
		case "done":
			s.Done = count
		}
	}
	return s, nil
}
