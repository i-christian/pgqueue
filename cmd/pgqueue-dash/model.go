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
	tabCron
)

// model represents the total state of the dashboard.
type model struct {
	db            *sql.DB
	activeTab     tab
	pollInterval  time.Duration
	overviewTable table.Model
	taskTable     table.Model
	cronTable     table.Model
	searchInput   textinput.Model
	detailView    viewport.Model
	showDetail    bool
	searching     bool
	confirming    bool
	activeConns   int
	lastUpdated   time.Time
	err           error

	taskPage      int
	totalTasks    int
	cronPage      int
	totalCronJobs int
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

	ct := table.New(table.WithColumns([]table.Column{
		{Title: "Job ID", Width: 32},
		{Title: "Job Name", Width: 25},
		{Title: "Schedule", Width: 15},
		{Title: "Last Run", Width: 20},
		{Title: "Next Run", Width: 20},
	}), table.WithHeight(15))

	ti := textinput.New()
	ti.Placeholder = "Filter..."

	return model{
		db:            db,
		overviewTable: ot,
		taskTable:     tt,
		cronTable:     ct,
		pollInterval:  interval,
		searchInput:   ti,
		activeTab:     tabOverview,
		taskPage:      0,
		cronPage:      0,
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
				m.taskPage = 0
				return m, m.fetchData()
			}
			m.searchInput, cmd = m.searchInput.Update(msg)
			return m, cmd
		}
		if m.confirming {
			switch msg.String() {
			case "y", "Y":
				m.confirming = false
				return m, m.retryTask()
			case "n", "N", "esc":
				m.confirming = false
				return m, nil
			}
			return m, nil
		}

		switch msg.String() {
		case "q", "ctrl+c":
			return m, tea.Quit
		case "tab", "right":
			m.activeTab = (m.activeTab + 1) % 3
			return m, m.fetchData()
		case "left":
			m.activeTab = (m.activeTab + 2) % 3
		case "/":
			if m.activeTab == tabTasks {
				m.searching = true
				return m, m.searchInput.Focus()
			}
		case "n":
			if m.activeTab == tabTasks && (m.taskPage+1)*pageSize < m.totalTasks {
				m.taskPage++
				return m, m.fetchData()
			}
			if m.activeTab == tabCron && (m.cronPage+1)*pageSize < m.totalCronJobs {
				m.cronPage++
				return m, m.fetchData()
			}
		case "p":
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
		case "r":
			if m.activeTab == tabTasks && !m.showDetail && !m.searching {
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
			m.overviewTable.SetRows(msg.overviewRows)
			m.taskTable.SetRows(msg.taskRows)
			m.cronTable.SetRows(msg.cronRows)
			m.activeConns = msg.activeConns
			m.totalTasks = msg.totalTasks
		}
	case string:
		m.detailView = viewport.New(85, 20)
		m.detailView.SetContent(msg)
		m.showDetail = true
	}

	if !m.showDetail && !m.searching {
		if m.activeTab == tabTasks {
			m.taskTable, cmd = m.taskTable.Update(msg)
		} else if m.activeTab == tabCron {
			m.cronTable, cmd = m.cronTable.Update(msg)
		}
	}

	return m, cmd
}
