package main

import (
	"fmt"
	"math"
	"strings"

	"github.com/charmbracelet/lipgloss"
)

var (
	cPrimary   = lipgloss.Color("62")  // Indigo/Purple
	cSecondary = lipgloss.Color("39")  // Deep Sky Blue
	cSuccess   = lipgloss.Color("42")  // Spring Green
	cWarning   = lipgloss.Color("214") // Orange
	cError     = lipgloss.Color("196") // Red
	cSubtle    = lipgloss.Color("241") // Grey

	stylePopup = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(cPrimary).
			Padding(1, 1).
			Background(lipgloss.Color("234"))

	styleTabActive = lipgloss.NewStyle().
			Border(lipgloss.NormalBorder(), false, false, true, false).
			BorderForeground(cPrimary).
			Foreground(cPrimary).
			Bold(true).
			Padding(0, 1)

	styleTabInactive = lipgloss.NewStyle().
				Padding(0, 1).
				Foreground(cSubtle)
)

func (m model) View() string {
	header := lipgloss.JoinHorizontal(lipgloss.Center,
		lipgloss.NewStyle().Foreground(cPrimary).Bold(true).Render("⚡ PGQUEUE"),
		lipgloss.NewStyle().Foreground(cSubtle).MarginLeft(2).Render("|"),
		lipgloss.NewStyle().Foreground(cSuccess).MarginLeft(2).Render("● Connected"),
		lipgloss.NewStyle().Foreground(cSubtle).MarginLeft(1).Render(fmt.Sprintf("(%d active)", m.activeConns)),
	)

	tabs := m.renderTabs()

	var content string
	switch m.activeTab {
	case tabOverview:
		content = m.renderOverview()
	case tabTasks:
		content = m.renderTaskView()
	case tabCron:
		content = m.renderCronView()
	}

	footer := m.renderFooter()

	base := lipgloss.JoinVertical(lipgloss.Left,
		lipgloss.NewStyle().Padding(0, 1).Render(header),
		lipgloss.NewStyle().Padding(0, 1).Render(tabs),
		lipgloss.NewStyle().Padding(0, 2).Render(content),
		footer,
	)

	if m.confirming {
		return m.overlay(m.renderConfirmation())
	}
	if m.showDetail {
		return m.overlay(m.renderDetailModal())
	}

	return base
}

func (m model) renderTabs() string {
	var t1, t2, t3 string
	if m.activeTab == tabOverview {
		t1 = styleTabActive.Render("OVERVIEW")
	} else {
		t1 = styleTabInactive.Render("OVERVIEW")
	}
	if m.activeTab == tabTasks {
		t2 = styleTabActive.Render("TASKS")
	} else {
		t2 = styleTabInactive.Render("TASKS")
	}
	if m.activeTab == tabCron {
		t3 = styleTabActive.Render("CRON JOBS")
	} else {
		t3 = styleTabInactive.Render("CRON JOBS")
	}

	row := lipgloss.JoinHorizontal(lipgloss.Bottom, t1, t2, t3)
	return lipgloss.NewStyle().MarginTop(1).Border(lipgloss.NormalBorder(), false, false, true, false).BorderForeground(cSubtle).Width(m.width - 4).Render(row)
}

// renderOverview replaces the grid with a System Monitor style layout
func (m model) renderOverview() string {
	label := lipgloss.NewStyle().Foreground(cSubtle).Width(14).Render
	val := lipgloss.NewStyle().Foreground(lipgloss.Color("252")).Bold(true).Render

	metrics := []string{
		fmt.Sprintf("%s %s", label("Total Tasks:"), val(fmt.Sprintf("%d", m.stats.Total))),
		fmt.Sprintf("%s %s", label("Pending:"), val(fmt.Sprintf("%d", m.stats.Pending))),
		fmt.Sprintf("%s %s", label("Processing:"), lipgloss.NewStyle().Foreground(cSecondary).Render(fmt.Sprintf("%d", m.stats.Processing))),
		fmt.Sprintf("%s %s", label("Retrying:"), lipgloss.NewStyle().Foreground(cWarning).Render(fmt.Sprintf("%d", m.stats.Retry))),
		"",
		fmt.Sprintf("%s %s", label("Failed:"), lipgloss.NewStyle().Foreground(cError).Render(fmt.Sprintf("%d", m.stats.Failed))),
		fmt.Sprintf("%s %s", label("Completed:"), lipgloss.NewStyle().Foreground(cSuccess).Render(fmt.Sprintf("%d", m.stats.Completed))),
	}
	leftCol := lipgloss.JoinVertical(lipgloss.Left, metrics...)

	total := float64(m.stats.Total)
	if total == 0 {
		total = 1
	}

	barWidth := 40
	makeBar := func(count int, color lipgloss.Color) string {
		pct := float64(count) / total
		w := int(pct * float64(barWidth))
		if w == 0 && count > 0 {
			w = 1
		}
		filled := strings.Repeat("█", w)
		empty := strings.Repeat("░", barWidth-w)
		return lipgloss.NewStyle().Foreground(color).Render(filled) + lipgloss.NewStyle().Foreground(cSubtle).Render(empty)
	}

	rightCol := lipgloss.JoinVertical(lipgloss.Left,
		lipgloss.NewStyle().Bold(true).Render("Status Distribution"),
		"",
		"Success Rate "+makeBar(m.stats.Completed, cSuccess)+fmt.Sprintf(" %.1f%%", (float64(m.stats.Completed)/total)*100),
		"Failure Rate "+makeBar(m.stats.Failed, cError)+fmt.Sprintf(" %.1f%%", (float64(m.stats.Failed)/total)*100),
		"Pending Load "+makeBar(m.stats.Pending, cSubtle)+fmt.Sprintf(" %.1f%%", (float64(m.stats.Pending)/total)*100),
	)

	return lipgloss.JoinHorizontal(lipgloss.Top,
		lipgloss.NewStyle().Width(35).Render(leftCol),
		lipgloss.NewStyle().MarginLeft(4).Render(rightCol),
	)
}

func (m model) renderTaskView() string {
	view := m.taskTable.View()

	if m.searching || m.searchInput.Value() != "" {
		sBar := lipgloss.NewStyle().
			Border(lipgloss.NormalBorder(), false, false, true, false).
			BorderForeground(cSubtle).
			Render("🔍 " + m.searchInput.View())
		return lipgloss.JoinVertical(lipgloss.Left, sBar, view)
	}

	totalPages := int(math.Ceil(float64(m.totalTasks) / float64(defaultPageSize)))
	if totalPages == 0 {
		totalPages = 1
	}

	pg := lipgloss.NewStyle().Foreground(cSubtle).MarginTop(1).Render(
		fmt.Sprintf("Page %d of %d  •  %d Total Tasks", m.taskPage+1, totalPages, m.totalTasks),
	)
	return lipgloss.JoinVertical(lipgloss.Left, view, pg)
}

func (m model) renderCronView() string {
	view := m.cronTable.View()

	totalPages := int(math.Ceil(float64(m.totalCronJobs) / float64(defaultPageSize)))
	if totalPages == 0 {
		totalPages = 1
	}

	pg := lipgloss.NewStyle().Foreground(cSubtle).MarginTop(1).Render(
		fmt.Sprintf("Page %d of %d  •  %d Cron Jobs", m.cronPage+1, totalPages, m.totalCronJobs),
	)
	return lipgloss.JoinVertical(lipgloss.Left, view, pg)
}

func (m model) renderFooter() string {
	var helps []string

	if m.showDetail {
		helps = []string{"ESC: Close", "↑/↓: Scroll"}
	} else if m.searching {
		helps = []string{"Enter: Confirm", "ESC: Cancel"}
	} else {
		helps = []string{"Tab: Switch View", "Q: Quit"}

		switch m.activeTab {
		case tabTasks:
			helps = append(helps, "←/→: Page", "/: Filter", "Enter: Details", "R: Retry")
		case tabCron:
			helps = append(helps, "←/→: Page", "Enter: Details")
		}
	}

	status := strings.Join(helps, " • ")

	return lipgloss.NewStyle().
		Foreground(cSubtle).
		PaddingTop(1).
		PaddingLeft(1).
		Render(status)
}

func (m model) renderDetailModal() string {
	w := int(math.Min(float64(m.width-10), 80))
	h := int(math.Min(float64(m.height-6), 30))

	header := lipgloss.NewStyle().
		Bold(true).
		Foreground(cPrimary).
		Border(lipgloss.NormalBorder(), false, false, true, false).
		BorderForeground(cSubtle).
		Width(w - 4).
		Render(m.detailTitle)

	m.detailView.Width = w - 4
	m.detailView.Height = h - 6

	return lipgloss.Place(m.width, m.height, lipgloss.Center, lipgloss.Center,
		stylePopup.Width(w).Height(h).Render(
			lipgloss.JoinVertical(lipgloss.Left,
				header,
				m.detailView.View(),
				lipgloss.NewStyle().Foreground(cSubtle).MarginTop(1).Render("↑/↓ Scroll • ESC Close"),
			),
		),
	)
}

func (m model) renderConfirmation() string {
	return lipgloss.Place(m.width, m.height, lipgloss.Center, lipgloss.Center,
		stylePopup.Render(lipgloss.JoinVertical(lipgloss.Center,
			lipgloss.NewStyle().Foreground(cWarning).Bold(true).Render("⚠️  RETRY TASK?"),
			"\nThis will reset status to 'pending'.",
			"\n",
			lipgloss.JoinHorizontal(lipgloss.Center,
				lipgloss.NewStyle().Foreground(cSuccess).MarginRight(2).Render("[Y] Yes"),
				lipgloss.NewStyle().Foreground(cError).Render("[N] No"),
			),
		)),
	)
}

func (m model) overlay(content string) string {
	return content
}
