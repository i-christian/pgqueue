package main

import (
	"fmt"

	"github.com/charmbracelet/lipgloss"
)

var (
	subtleStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("241"))
	successStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("10"))
	pageStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("241")).Italic(true)

	headerStyle = lipgloss.NewStyle().
			Background(lipgloss.Color("62")).
			Foreground(lipgloss.Color("230")).
			Padding(0, 1).
			Bold(true)

	statBoxStyle = lipgloss.NewStyle().
			Padding(0, 1).
			Border(lipgloss.NormalBorder(), false, true, false, false).
			BorderForeground(lipgloss.Color("240"))

	activeTabStyle = lipgloss.NewStyle().
			Border(lipgloss.NormalBorder(), false, false, true, false).
			BorderForeground(lipgloss.Color("62")).
			Foreground(lipgloss.Color("62")).
			Bold(true).
			Padding(0, 2)

	inactiveTabStyle = lipgloss.NewStyle().Padding(0, 2).Foreground(lipgloss.Color("244"))

	popupStyle = lipgloss.NewStyle().
			Border(lipgloss.DoubleBorder()).
			BorderForeground(lipgloss.Color("62")).
			Padding(1).
			Background(lipgloss.Color("234"))
)

// View renders the final string based on the current model state.
func (m model) View() string {
	statusText := successStyle.Render("CONNECTED")
	topBar := lipgloss.JoinHorizontal(lipgloss.Center,
		headerStyle.Render(" PGQUEUE CONSOLE "),
		statBoxStyle.Render(fmt.Sprintf("STATUS: %s", statusText)),
		statBoxStyle.Render(fmt.Sprintf("WKR CONNS: %d", m.activeConns)),
	)

	tabs := []string{
		inactiveTabStyle.Render("OVERVIEW"),
		inactiveTabStyle.Render("TASK LIST"),
		inactiveTabStyle.Render("CRON JOBS"),
	}

	switch m.activeTab {
	case tabOverview:
		tabs[0] = activeTabStyle.Render("OVERVIEW")
	case tabTasks:
		tabs[1] = activeTabStyle.Render("TASK LIST")
	default:
		tabs[2] = activeTabStyle.Render("CRON JOBS")
	}
	tabRow := lipgloss.JoinHorizontal(lipgloss.Top, tabs...)

	var content string
	switch m.activeTab {
	case tabOverview:
		content = lipgloss.JoinVertical(lipgloss.Left,
			lipgloss.NewStyle().Foreground(lipgloss.Color("62")).Bold(true).Render("\n SYSTEM METRICS"),
			m.overviewTable.View(),
		)
	case tabTasks:
		pagination := pageStyle.Render(fmt.Sprintf(" Task Page %d/%d • [n]ext/[p]rev • [/] search", m.taskPage+1, (m.totalTasks/pageSize)+1))
		content = lipgloss.JoinVertical(lipgloss.Left, m.searchInput.View(), "\n", m.taskTable.View(), "\n", pagination)
	case tabCron:
		pagination := pageStyle.Render(fmt.Sprintf(" Cron Page %d/%d • [n]ext/[p]rev", m.cronPage+1, (m.totalCronJobs/pageSize)+1))
		content = lipgloss.JoinVertical(lipgloss.Left, "\n", m.cronTable.View(), "\n", pagination)
	}

	var statusLine string
	if m.err != nil {
		statusLine = lipgloss.NewStyle().
			Background(lipgloss.Color("9")).
			Foreground(lipgloss.Color("15")).
			Width(100).
			Render(fmt.Sprintf(" ⚠️  DATABASE ERROR: %v", m.err))
	} else {
		statusLine = subtleStyle.Render(fmt.Sprintf(" [Tab] Tab • [/] Filter • [Enter] Detail • [R] Retry • [Q] Quit | Last Sync: %s", m.lastUpdated.Format("15:04:05")))
	}

	view := lipgloss.JoinVertical(lipgloss.Left, topBar, "\n", tabRow, "\n", content, statusLine)

	if m.confirming {
		prompt := lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color("196")).
			Padding(1, 2).
			Background(lipgloss.Color("234")).
			Render("⚠️  RETRY TASK?\n\nThis will re-queue the task for immediate execution.\n\n[y] Confirm  •  [n] Cancel")

		return lipgloss.Place(100, 40, lipgloss.Center, lipgloss.Center, prompt)
	}

	if m.showDetail {
		return m.renderDetailModal()
	}
	return view
}

// renderDetailModal constructs a structured view for task introspection.
// It uses a viewport for the JSON payload to allow scrolling for large data.
func (m model) renderDetailModal() string {
	selected := m.taskTable.SelectedRow()
	if len(selected) == 0 {
		return ""
	}

	header := headerStyle.Render(fmt.Sprintf(" 🔍 TASK DETAILS: %s ", selected[0]))

	modalContent := lipgloss.JoinVertical(
		lipgloss.Left,
		header,
		"\n",
		m.detailView.View(),
		"\n",
		subtleStyle.Render(" [ESC] Back • [↑/↓] Scroll Payload "),
	)

	return lipgloss.Place(
		50, 20,
		lipgloss.Center, lipgloss.Center,
		popupStyle.Render(modalContent),
	)
}
