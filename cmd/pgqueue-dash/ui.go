package main

import (
	"fmt"

	"github.com/charmbracelet/lipgloss"
)

var (
	subtleStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("241"))
	prioStyle    = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("12"))
	errorStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("9"))
	successStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("10"))
	warnStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("214"))
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

func (m model) View() string {
	statusText := successStyle.Render("CONNECTED")
	topBar := lipgloss.JoinHorizontal(lipgloss.Center,
		headerStyle.Render(" PGQUEUE CONSOLE "),
		statBoxStyle.Render(fmt.Sprintf("STATUS: %s", statusText)),
		statBoxStyle.Render(fmt.Sprintf("WKR CONNS: %d", m.activeConns)),
	)

	tabs := []string{inactiveTabStyle.Render("OVERVIEW"), inactiveTabStyle.Render("TASK LIST")}
	if m.activeTab == tabOverview {
		tabs[0] = activeTabStyle.Render("OVERVIEW")
	} else {
		tabs[1] = activeTabStyle.Render("TASK LIST")
	}
	tabRow := lipgloss.JoinHorizontal(lipgloss.Top, tabs...)

	var content string
	if m.activeTab == tabOverview {
		content = m.overviewTable.View()
	} else {
		pagination := pageStyle.Render(fmt.Sprintf("Page %d (Total: %d) • [n]ext/[p]rev", m.currentPage+1, m.totalTasks))
		content = lipgloss.JoinVertical(lipgloss.Left, m.searchInput.View(), "\n", m.taskTable.View(), "\n", pagination)
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

	if m.showDetail {
		return lipgloss.Place(100, 40, lipgloss.Center, lipgloss.Center, popupStyle.Render(m.detailView.View()+"\n\n"+subtleStyle.Render("[ESC] Return")))
	}
	return view
}
