package main

import (
	"database/sql"
	"time"

	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/bubbles/textinput"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
)

const (
	pageSize      = 25
	envConnString = "PG_CONN_STRING"
)

type tab int

const (
	tabOverview tab = iota
	tabTasks
)

// model represents the total state of the dashboard.
type model struct {
	db            *sql.DB
	activeTab     tab
	pollInterval  time.Duration
	overviewTable table.Model
	taskTable     table.Model
	searchInput   textinput.Model
	detailView    viewport.Model
	showDetail    bool
	searching     bool
	activeConns   int
	lastUpdated   time.Time
	err           error

	currentPage int
	totalTasks  int
}

// initialModel sets up the UI components with default dimensions and styling.
func initialModel(db *sql.DB, interval time.Duration) model {
	ot := table.New(table.WithColumns([]table.Column{
		{Title: "Metric Group", Width: 25},
		{Title: "Count", Width: 12},
	}), table.WithHeight(10))

	tt := table.New(table.WithColumns([]table.Column{
		{Title: "Short ID", Width: 10},
		{Title: "Task Type", Width: 20},
		{Title: "Status", Width: 12},
		{Title: "Prio", Width: 6},
		{Title: "Last Error", Width: 45},
	}), table.WithHeight(15), table.WithFocused(true))

	ti := textinput.New()
	ti.Placeholder = "Filter..."

	return model{
		db:            db,
		overviewTable: ot,
		taskTable:     tt,
		pollInterval:  interval,
		searchInput:   ti,
		activeTab:     tabOverview,
		currentPage:   0,
	}
}

// Init kicks off the first data fetch and the recurring tick timer.
func (m model) Init() tea.Cmd {
	return tea.Batch(
		tea.Tick(m.pollInterval, func(t time.Time) tea.Msg { return TickMsg(t) }),
		m.fetchData(),
	)
}

// Update handles all incoming messages (ticks, keys, data updates)
// and returns the updated state and any side-effect commands.
func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd
	switch msg := msg.(type) {
	case tea.KeyMsg:
		if m.showDetail {
			if msg.String() == "esc" {
				m.showDetail = false
			}
			return m, nil
		}
		if m.searching {
			if msg.String() == "enter" || msg.String() == "esc" {
				m.searching = false
				m.searchInput.Blur()
				m.currentPage = 0
				return m, m.fetchData()
			}
			m.searchInput, cmd = m.searchInput.Update(msg)
			return m, cmd
		}
		switch msg.String() {
		case "q", "ctrl+c":
			return m, tea.Quit
		case "tab", "right", "left":
			m.activeTab = (m.activeTab + 1) % 2
		case "/":
			m.searching = true
			return m, m.searchInput.Focus()
		case "n":
			if (m.currentPage+1)*pageSize < m.totalTasks {
				m.currentPage++
				return m, m.fetchData()
			}
		case "p":
			if m.currentPage > 0 {
				m.currentPage--
				return m, m.fetchData()
			}
		case "enter":
			if m.activeTab == tabTasks {
				return m, m.showTaskDetail()
			}
		case "r":
			if m.activeTab == tabTasks {
				return m, m.retryTask()
			}
		}
	case TickMsg:
		m.lastUpdated = time.Time(msg)
		return m, tea.Batch(tea.Tick(m.pollInterval, func(t time.Time) tea.Msg { return TickMsg(t) }), m.fetchData())
	case DataUpdateMsg:
		m.err = msg.Err
		if msg.Err == nil {
			m.overviewTable.SetRows(msg.overviewRows)
			m.taskTable.SetRows(msg.taskRows)
			m.activeConns = msg.activeConns
			m.totalTasks = msg.totalTasks
		}
	case string:
		m.detailView = viewport.New(85, 20)
		m.detailView.SetContent(msg)
		m.showDetail = true
	}
	if m.activeTab == tabTasks && !m.showDetail && !m.searching {
		m.taskTable, cmd = m.taskTable.Update(msg)
	}
	return m, cmd
}
