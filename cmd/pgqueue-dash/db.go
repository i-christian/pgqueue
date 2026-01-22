package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/charmbracelet/bubbles/table"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/google/uuid"
)

type (
	TickMsg       time.Time
	DataUpdateMsg struct {
		overviewRows  []table.Row
		taskRows      []table.Row
		cronRows      []table.Row
		activeConns   int
		totalTasks    int
		totalCronJobs int
		Err           error
	}
)

// fetchData retrieves metrics and tasks with pagination support
func (m model) fetchData() tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		var conns, totalT, totalC int
		err := m.db.QueryRowContext(ctx, "SELECT count(*) FROM pg_stat_activity").Scan(&conns)
		if err != nil {
			return DataUpdateMsg{Err: err}
		}

		filter := "%" + m.searchInput.Value() + "%"
		_ = m.db.QueryRowContext(ctx, "SELECT count(*) FROM tasks WHERE task_type ILIKE $1", filter).Scan(&totalT)
		_ = m.db.QueryRowContext(ctx, "SELECT count(*) FROM cron_jobs WHERE name ILIKE $1", filter).Scan(&totalC)

		oRows, _ := m.db.QueryContext(ctx, `(SELECT 'Status: ' || status, count(*) FROM tasks GROUP BY status) UNION ALL (SELECT 'Priority: ' || priority::text, count(*) FROM tasks GROUP BY priority) ORDER BY 1`)
		defer oRows.Close()
		var ov []table.Row
		for oRows.Next() {
			var cat string
			var count int
			oRows.Scan(&cat, &count)
			ov = append(ov, table.Row{cat, fmt.Sprintf("%d", count)})
		}

		tOffset := m.taskPage * pageSize
		tRows, _ := m.db.QueryContext(ctx, `
			SELECT task_id, task_type, status, priority, LEFT(COALESCE(last_error, '-'), 45)
			FROM tasks WHERE (task_type ILIKE $1 OR last_error ILIKE $1)
			ORDER BY created_at DESC LIMIT $2 OFFSET $3
		`, filter, pageSize, tOffset)
		defer tRows.Close()
		var tv []table.Row
		for tRows.Next() {
			var id, tType, status, errStr string
			var prio int
			tRows.Scan(&id, &tType, &status, &prio, &errStr)
			tv = append(tv, table.Row{id[:8], tType, status, fmt.Sprintf("%d", prio), errStr})
		}

		cOffset := m.cronPage * pageSize
		cRows, err := m.db.QueryContext(ctx, `
			SELECT job_id, name, expression, last_run_at, next_run_at 
			FROM cron_jobs WHERE name ILIKE $1
			ORDER BY name ASC LIMIT $2 OFFSET $3
		`, filter, pageSize, cOffset)

		var cv []table.Row
		if err == nil {
			defer cRows.Close()
			for cRows.Next() {
				var jobID uuid.UUID
				var name, expr string
				var last, next sql.NullTime
				cRows.Scan(&jobID, &name, &expr, &last, &next)

				lStr := subtleStyle.Render("-")
				if last.Valid {
					lStr = last.Time.Format(time.DateTime)
				}
				nStr := successStyle.Render("-")
				if next.Valid {
					nStr = next.Time.Format(time.DateTime)
				}

				cv = append(cv, table.Row{jobID.String()[:8], name, expr, lStr, nStr})
			}
		}

		return DataUpdateMsg{ov, tv, cv, conns, totalT, totalC, nil}
	}
}

// showTaskDetail fetches the full JSON payload and error string for the selected task.
// It formats the output with Lipgloss styles before populating the viewport.
func (m model) showTaskDetail() tea.Cmd {
	return func() tea.Msg {
		selected := m.taskTable.SelectedRow()
		if len(selected) == 0 {
			return nil
		}

		var payload []byte
		var lastErr sql.NullString
		err := m.db.QueryRow("SELECT payload, last_error FROM tasks WHERE task_id::text LIKE $1", selected[0]+"%").Scan(&payload, &lastErr)
		if err != nil {
			return fmt.Sprintf("Error fetching details: %v", err)
		}

		var raw json.RawMessage = payload
		prettyJSON, _ := json.MarshalIndent(raw, "", "  ")

		label := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("62"))
		valStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("252"))
		errStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("9"))

		return fmt.Sprintf(
			"%s %s\n\n%s\n%s\n\n%s\n%s",
			label.Render("TASK ID:"), valStyle.Render(selected[0]),
			label.Render("LAST RECORDED ERROR:"), errStyle.Render(lastErr.String),
			label.Render("DATA PAYLOAD:"), valStyle.Render(string(prettyJSON)),
		)
	}
}

// retryTask resets a task to 'pending' and sets its next run time to NOW.
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
