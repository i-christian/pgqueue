package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/table"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/google/uuid"
)

type (
	TickMsg          time.Time
	DetailContentMsg struct {
		Title   string
		Content string
	}
	DataUpdateMsg struct {
		Stats         DashboardStats
		taskRows      []table.Row
		cronRows      []table.Row
		activeConns   int
		totalTasks    int
		totalCronJobs int
		Err           error
	}
)

func (m model) fetchData() tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		var stats DashboardStats
		var conns, totalT, totalC int

		_ = m.db.QueryRowContext(ctx, "SELECT count(*) FROM pg_stat_activity").Scan(&conns)

		qStats := `
			SELECT 
				count(*) FILTER (WHERE status = 'pending'),
				count(*) FILTER (WHERE status = 'processing'),
				count(*) FILTER (WHERE status = 'done'),
				count(*) FILTER (WHERE status = 'failed'),
				count(*) FILTER (WHERE attempts > 0 AND status = 'pending')
			FROM pgqueue.tasks
		`
		if err := m.db.QueryRowContext(ctx, qStats).Scan(
			&stats.Pending, &stats.Processing, &stats.Completed, &stats.Failed, &stats.Retry,
		); err != nil {
			return DataUpdateMsg{Err: err}
		}

		stats.Total = stats.Pending + stats.Processing + stats.Completed + stats.Failed
		search := strings.TrimSpace(m.searchInput.Value())

		if search != "" {
			qCount := `
				SELECT count(*)
				FROM pgqueue.tasks
				WHERE
					to_tsvector('simple',
						coalesce(task_type,'') || ' ' ||
						coalesce(last_error,'')
					) @@ websearch_to_tsquery('simple', $1)
			`
			_ = m.db.QueryRowContext(ctx, qCount, search).Scan(&totalT)
		} else {
			totalT = stats.Total
		}
		_ = m.db.QueryRowContext(ctx, "SELECT count(*) FROM pgqueue.cron_jobs").Scan(&totalC)

		tOffset := m.taskPage * defaultPageSize
		tRowsData, _ := m.db.QueryContext(ctx, `
		SELECT
			task_id,
			task_type,
			status,
			priority,
			LEFT(COALESCE(last_error, '-'), 50)
		FROM pgqueue.tasks
		WHERE
			($1 = '' OR
				to_tsvector('simple',
					coalesce(task_type,'') || ' ' ||
					coalesce(last_error,'')
				) @@ websearch_to_tsquery('simple', $1)
			)
		ORDER BY
			created_at DESC
		LIMIT $2 OFFSET $3
		`, search, defaultPageSize, tOffset)

		var tv []table.Row
		if tRowsData != nil {
			defer tRowsData.Close()
			for tRowsData.Next() {
				var id uuid.UUID
				var tType, status, errStr string
				var prio int
				tRowsData.Scan(&id, &tType, &status, &prio, &errStr)
				tv = append(tv, table.Row{id.String(), tType, status, fmt.Sprintf("%d", prio), errStr})
			}
		}

		cOffset := m.cronPage * defaultPageSize
		cRowsData, err := m.db.QueryContext(ctx, `
			SELECT job_id, name, expression, last_run_at, next_run_at 
			FROM pgqueue.cron_jobs 
			ORDER BY name ASC LIMIT $1 OFFSET $2
		`, defaultPageSize, cOffset)

		var cv []table.Row
		if err == nil && cRowsData != nil {
			defer cRowsData.Close()
			for cRowsData.Next() {
				var jobID uuid.UUID
				var name, expr string
				var last, next sql.NullTime
				cRowsData.Scan(&jobID, &name, &expr, &last, &next)

				lStr := "-"
				if last.Valid {
					lStr = last.Time.Format("2006-01-02 15:04:05")
				}
				nStr := "-"
				if next.Valid {
					nStr = next.Time.Format("2006-01-02 15:04:05")
				}

				cv = append(cv, table.Row{jobID.String(), name, expr, lStr, nStr})
			}
		}

		return DataUpdateMsg{stats, tv, cv, conns, totalT, totalC, nil}
	}
}

func (m model) showTaskDetail() tea.Cmd {
	return func() tea.Msg {
		selected := m.taskTable.SelectedRow()
		if len(selected) == 0 {
			return nil
		}
		id := strings.TrimSpace(selected[0])

		createdAt, err := extractTimeFromUUIDv7(id)
		if err != nil {
			return DetailContentMsg{Title: "Error", Content: "invalid UUID format"}
		}

		var payload []byte
		var lastErr sql.NullString
		var created, nextRun sql.NullTime
		var attempts int

		err = m.db.QueryRow(`
			SELECT payload, last_error, created_at, next_run_at, attempts 
			FROM pgqueue.tasks 
			WHERE task_id = $1 AND created_at = $2`, id, createdAt).
			Scan(&payload, &lastErr, &created, &nextRun, &attempts)
		if err != nil {
			return DetailContentMsg{Title: "Error", Content: err.Error()}
		}

		lbl := lipgloss.NewStyle().Foreground(lipgloss.Color("241")).Render
		val := lipgloss.NewStyle().Foreground(lipgloss.Color("252")).Bold(true).Render
		errVal := lipgloss.NewStyle().Foreground(lipgloss.Color("196")).Render

		infoBlock := fmt.Sprintf("%s %s    %s %s    %s %d",
			lbl("Created:"), val(created.Time.Format(time.RFC822)),
			lbl("Next Run:"), val(nextRun.Time.Format(time.RFC822)),
			lbl("Attempts:"), attempts,
		)

		errBlock := ""
		if lastErr.Valid && lastErr.String != "" {
			errBlock = fmt.Sprintf("\n%s\n%s\n", lbl("LAST ERROR:"), errVal(lastErr.String))
		}

		prettyJSON := highlightJSON(payload)

		content := fmt.Sprintf("%s\n%s\n%s\n%s", infoBlock, errBlock, lbl("PAYLOAD:"), prettyJSON)
		return DetailContentMsg{Title: fmt.Sprintf("TASK ID: %s", id), Content: content}
	}
}

func (m model) showCronDetail() tea.Cmd {
	return func() tea.Msg {
		selected := m.cronTable.SelectedRow()
		if len(selected) == 0 {
			return nil
		}
		id := selected[0]

		var name, expression string
		var lastRun, nextRun, created sql.NullTime

		err := m.db.QueryRow(`
			SELECT name, expression, last_run_at, next_run_at, created_at 
			FROM pgqueue.cron_jobs WHERE job_id = $1`, id).
			Scan(&name, &expression, &lastRun, &nextRun, &created)
		if err != nil {
			return DetailContentMsg{Title: "Error", Content: err.Error()}
		}

		lbl := lipgloss.NewStyle().Foreground(lipgloss.Color("241")).Width(12).Render
		val := lipgloss.NewStyle().Foreground(lipgloss.Color("252")).Render

		lStr := "-"
		if lastRun.Valid {
			lStr = lastRun.Time.Format(time.RFC1123)
		}
		nStr := "-"
		if nextRun.Valid {
			nStr = nextRun.Time.Format(time.RFC1123)
		}

		content := fmt.Sprintf("%s %s\n%s %s\n%s %s\n\n%s %s\n%s %s",
			lbl("Name:"), val(name),
			lbl("Cron Expr:"), lipgloss.NewStyle().Foreground(lipgloss.Color("220")).Render(expression),
			lbl("Created:"), val(created.Time.Format(time.RFC1123)),
			lbl("Last Run:"), val(lStr),
			lbl("Next Run:"), lipgloss.NewStyle().Foreground(lipgloss.Color("42")).Render(nStr),
		)

		return DetailContentMsg{Title: "CRON JOB DETAILS", Content: content}
	}
}

func (m model) retryTask() tea.Cmd {
	return func() tea.Msg {
		sel := m.taskTable.SelectedRow()
		if len(sel) == 0 {
			return nil
		}
		id := strings.TrimSpace(sel[0])

		createdAt, err := extractTimeFromUUIDv7(id)
		if err != nil {
			return nil
		}

		m.db.Exec(`
			UPDATE pgqueue.tasks 
			SET status='pending', attempts=0, next_run_at=NOW(), last_error=NULL 
			WHERE task_id = $1 AND created_at = $2
		`, id, createdAt)
		return nil
	}
}

// highlightJSON is a manual tokenizer to pretty print JSON
func highlightJSON(data []byte) string {
	var v any
	if err := json.Unmarshal(data, &v); err != nil {
		return string(data)
	}

	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetIndent("", "  ")
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(v)

	lines := strings.Split(buf.String(), "\n")
	var out []string

	keyStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("39"))   // Blue
	strStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("78"))   // Green
	numStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("208"))  // Orange
	boolStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("201")) // Pink

	for _, line := range lines {
		if strings.Contains(line, ":") {
			parts := strings.SplitN(line, ":", 2)
			parts[0] = keyStyle.Render(parts[0])

			val := parts[1]
			if strings.Contains(val, `"`) {
				val = strStyle.Render(val)
			} else if strings.Contains(val, "true") || strings.Contains(val, "false") || strings.Contains(val, "null") {
				val = boolStyle.Render(val)
			} else if strings.TrimSpace(val) != "{" && strings.TrimSpace(val) != "[" {
				val = numStyle.Render(val)
			}
			parts[1] = val
			out = append(out, strings.Join(parts, ":"))
		} else {
			out = append(out, line)
		}
	}
	return strings.Join(out, "\n")
}
