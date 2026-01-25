package main

import (
	"database/sql"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/bubbles/textinput"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

const (
	defaultPageSize = 20
	envConnString   = "PG_CONN_STRING"
)

type tab int

const (
	tabOverview tab = iota
	tabTasks
	tabCron
)

type DashboardStats struct {
	Pending    int
	Processing int
	Completed  int
	Failed     int
	Retry      int
	Total      int
}

type model struct {
	db           *sql.DB
	activeTab    tab
	pollInterval time.Duration

	width  int
	height int

	taskTable   table.Model
	cronTable   table.Model
	searchInput textinput.Model
	detailView  viewport.Model

	showDetail bool
	searching  bool
	confirming bool

	activeConns int
	stats       DashboardStats
	lastUpdated time.Time
	err         error

	taskPage      int
	totalTasks    int
	cronPage      int
	totalCronJobs int

	detailTitle string
}

func initialModel(db *sql.DB, interval time.Duration) model {
	tt := table.New(table.WithColumns([]table.Column{
		{Title: "ID", Width: 32},
		{Title: "Type", Width: 20},
		{Title: "Status", Width: 10},
		{Title: "Prio", Width: 5},
		{Title: "Last Error", Width: 45},
	}), table.WithFocused(true), table.WithHeight(15))

	s := table.DefaultStyles()
	s.Header = s.Header.BorderStyle(lipgloss.NormalBorder()).BorderForeground(lipgloss.Color("240")).BorderBottom(true).Bold(true)
	s.Selected = s.Selected.Foreground(lipgloss.Color("229")).Background(lipgloss.Color("62")).Bold(false)
	tt.SetStyles(s)

	ct := table.New(table.WithColumns([]table.Column{
		{Title: "ID", Width: 32},
		{Title: "Job Name", Width: 25},
		{Title: "Schedule", Width: 15},
		{Title: "Last Run", Width: 20},
		{Title: "Next Run", Width: 20},
	}), table.WithFocused(false), table.WithHeight(15))
	ct.SetStyles(s)

	ti := textinput.New()
	ti.Placeholder = "Filter by Task Type Or Error..."
	ti.CharLimit = 50
	ti.Width = 30

	vp := viewport.New(0, 0)

	return model{
		db:           db,
		taskTable:    tt,
		cronTable:    ct,
		detailView:   vp,
		pollInterval: interval,
		searchInput:  ti,
		activeTab:    tabOverview,
	}
}

func (m model) Init() tea.Cmd {
	return tea.Batch(
		tea.Tick(m.pollInterval, func(t time.Time) tea.Msg { return TickMsg(t) }),
		m.fetchData(),
	)
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd

	if msg, ok := msg.(tea.WindowSizeMsg); ok {
		m.width = msg.Width
		m.height = msg.Height
		availHeight := max(m.height-10, 5)
		m.taskTable.SetHeight(availHeight)
		m.cronTable.SetHeight(availHeight)
		m.detailView.Width = m.width - 4
		m.detailView.Height = m.height - 8
		return m, nil
	}

	if m.showDetail {
		switch msg := msg.(type) {
		case tea.KeyMsg:
			if msg.String() == "esc" {
				m.showDetail = false
				return m, nil
			}
		}
		m.detailView, cmd = m.detailView.Update(msg)
		return m, cmd
	}

	if m.searching {
		switch msg := msg.(type) {
		case tea.KeyMsg:
			if msg.String() == "enter" || msg.String() == "esc" {
				m.searching = false
				m.searchInput.Blur()
				m.taskTable.Focus()
				m.taskPage = 0
				return m, m.fetchData()
			}
		}
		m.searchInput, cmd = m.searchInput.Update(msg)
		return m, cmd
	}

	if m.confirming {
		switch msg := msg.(type) {
		case tea.KeyMsg:
			if strings.ToLower(msg.String()) == "y" {
				m.confirming = false
				return m, m.retryTask()
			} else if strings.ToLower(msg.String()) == "n" || msg.String() == "esc" {
				m.confirming = false
				return m, nil
			}
		}
		return m, nil
	}

	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "q", "ctrl+c":
			return m, tea.Quit

		case "tab":
			m.activeTab = (m.activeTab + 1) % 3
			m.updateTableFocus()
			return m, m.fetchData()

		case "shift+tab":
			m.activeTab--
			if m.activeTab < 0 {
				m.activeTab = tabCron
			}
			m.updateTableFocus()
			return m, m.fetchData()

		case "/":
			if m.activeTab == tabTasks {
				m.searching = true
				m.taskTable.Blur()
				return m, m.searchInput.Focus()
			}

		case "n", "right", "l":
			if m.activeTab == tabTasks && (m.taskPage+1)*defaultPageSize < m.totalTasks {
				m.taskPage++
				return m, m.fetchData()
			}
			if m.activeTab == tabCron && (m.cronPage+1)*defaultPageSize < m.totalCronJobs {
				m.cronPage++
				return m, m.fetchData()
			}

		case "p", "left", "h":
			if m.activeTab == tabTasks && m.taskPage > 0 {
				m.taskPage--
				return m, m.fetchData()
			}
			if m.activeTab == tabCron && m.cronPage > 0 {
				m.cronPage--
				return m, m.fetchData()
			}

		case "enter":
			if m.activeTab == tabTasks {
				return m, m.showTaskDetail()
			}
			if m.activeTab == tabCron {
				return m, m.showCronDetail()
			}

		case "r":
			if m.activeTab == tabTasks {
				m.confirming = true
				return m, nil
			}
		}

	case TickMsg:
		m.lastUpdated = time.Time(msg)
		return m, tea.Batch(tea.Tick(m.pollInterval, func(t time.Time) tea.Msg { return TickMsg(t) }), m.fetchData())

	case DataUpdateMsg:
		m.err = msg.Err
		if msg.Err == nil {
			m.stats = msg.Stats
			m.taskTable.SetRows(msg.taskRows)
			m.cronTable.SetRows(msg.cronRows)
			m.activeConns = msg.activeConns
			m.totalTasks = msg.totalTasks
			m.totalCronJobs = msg.totalCronJobs
		}

	case DetailContentMsg:
		m.detailView.GotoTop()
		m.detailView.SetContent(msg.Content)
		m.detailTitle = msg.Title
		m.showDetail = true
	}

	if !m.showDetail && !m.searching && !m.confirming {
		switch m.activeTab {
		case tabTasks:
			m.taskTable, cmd = m.taskTable.Update(msg)
		case tabCron:
			m.cronTable, cmd = m.cronTable.Update(msg)
		}
	}

	return m, cmd
}

// updateTableFocus ensures only the visible table captures input
func (m *model) updateTableFocus() {
	switch m.activeTab {
	case tabTasks:
		m.taskTable.Focus()
		m.cronTable.Blur()
	case tabCron:
		m.cronTable.Focus()
		m.taskTable.Blur()
	default:
		m.taskTable.Blur()
		m.cronTable.Blur()
	}
}
