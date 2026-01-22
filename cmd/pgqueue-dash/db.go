package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/charmbracelet/bubbles/table"
	tea "github.com/charmbracelet/bubbletea"
)

type (
	TickMsg       time.Time
	DataUpdateMsg struct {
		overviewRows []table.Row
		taskRows     []table.Row
		activeConns  int
		totalTasks   int
		Err          error
	}
)

// fetchData retrieves metrics and tasks with pagination support
func (m model) fetchData() tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		var conns, total int
		err := m.db.QueryRowContext(ctx, "SELECT count(*) FROM pg_stat_activity").Scan(&conns)
		if err != nil {
			return DataUpdateMsg{Err: err}
		}

		filter := "%" + m.searchInput.Value() + "%"
		_ = m.db.QueryRowContext(ctx, "SELECT count(*) FROM tasks WHERE task_type ILIKE $1", filter).Scan(&total)

		oRows, _ := m.db.QueryContext(ctx, `(SELECT 'Status: ' || status, count(*) FROM tasks GROUP BY status) UNION ALL (SELECT 'Priority: ' || priority::text, count(*) FROM tasks GROUP BY priority) ORDER BY 1`)
		defer oRows.Close()
		var ov []table.Row
		for oRows.Next() {
			var cat string
			var count int
			oRows.Scan(&cat, &count)
			ov = append(ov, table.Row{cat, fmt.Sprintf("%d", count)})
		}

		offset := m.currentPage * pageSize
		tRows, _ := m.db.QueryContext(ctx, `
			SELECT task_id, task_type, status, priority, LEFT(COALESCE(last_error, '-'), 45)
			FROM tasks WHERE (task_type ILIKE $1 OR last_error ILIKE $1)
			ORDER BY created_at DESC LIMIT $2 OFFSET $3
		`, filter, pageSize, offset)
		defer tRows.Close()
		var tv []table.Row
		for tRows.Next() {
			var id, tType, status, errStr string
			var prio int
			tRows.Scan(&id, &tType, &status, &prio, &errStr)
			tv = append(tv, table.Row{id[:8], tType, status, fmt.Sprintf("%d", prio), errStr})
		}

		return DataUpdateMsg{ov, tv, conns, total, nil}
	}
}

func (m model) showTaskDetail() tea.Cmd {
	return func() tea.Msg {
		selected := m.taskTable.SelectedRow()
		if len(selected) == 0 {
			return nil
		}
		var p []byte
		var e sql.NullString
		_ = m.db.QueryRow("SELECT payload, last_error FROM tasks WHERE task_id::text LIKE $1", selected[0]+"%").Scan(&p, &e)
		var pretty json.RawMessage = p
		fmtP, _ := json.MarshalIndent(pretty, "", "  ")
		return fmt.Sprintf("TASK: %s\nERROR: %s\n\nPAYLOAD:\n%s", selected[0], e.String, string(fmtP))
	}
}

func (m model) retryTask() tea.Cmd {
	return func() tea.Msg {
		sel := m.taskTable.SelectedRow()
		if len(sel) == 0 {
			return nil
		}
		_, _ = m.db.Exec("UPDATE tasks SET status='pending', attempts=0, next_run_at=NOW() WHERE task_id::text LIKE $1", sel[0]+"%")
		return nil
	}
}
