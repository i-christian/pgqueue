# pgqueue-dash 🐘

A production-ready, high-performance Terminal User Interface (TUI) for monitoring and operating the `pgqueue` task queue in PostgreSQL.

Built with **Golang**, **Bubble Tea**, **Bubbles**, and **Lip Gloss** for fast, keyboard-driven workflows.

---

## Features

- **Live Metrics**
  - Real-time task state distribution (pending, processing, failed, retrying, completed)

- **Powerful Search**
  - PostgreSQL **full-text search** over task type and error messages
  - Instant filtering without leaving the TUI

- **Task & Cron Inspection**
  - Scrollable detail modals
  - Pretty-printed, syntax-highlighted JSON payloads

- **Pagination at Scale**
  - Efficient browsing of millions of rows using `LIMIT / OFFSET`

- **Actionable Operations**
  - Retry failed tasks with confirmation

- **Keyboard-First UX**
  - Vim-style navigation (`h`, `j`, `k`, `l`)

---

## Installation

```bash
go install github.com/i-christian/pgqueue/cmd/pgqueue-dash@latest
````

---

## Keyboard Shortcuts

### Global

| Key            | Action       |
| -------------- | ------------ |
| `q` / `Ctrl+C` | Quit         |
| `Tab`          | Next tab     |
| `Shift+Tab`    | Previous tab |

### Navigation (Vim-compatible)

| Key       | Action        |
| --------- | ------------- |
| `j` / `↓` | Move down     |
| `k` / `↑` | Move up       |
| `h` / `←` | Previous page |
| `l` / `→` | Next page     |

### Tasks Tab

| Key     | Action                   |
| ------- | ------------------------ |
| `/`     | Search tasks (full-text) |
| `Enter` | View task details        |
| `r`     | Retry selected task      |
| `Esc`   | Close modal / cancel     |

### Cron Jobs Tab

| Key       | Action                |
| --------- | --------------------- |
| `Enter`   | View cron job details |
| `h` / `l` | Change page           |

---

## Screenshots 📸

### Overview
![Overview](docs/screenshots/overview.png)

### Tasks View
![Tasks](docs/screenshots/tasks.png)

### Task Detail (JSON Payload)
![Task Detail](docs/screenshots/task_detail.png)

---

## Configuration

Configuration can be provided via **environment variables** or **CLI flags**.
CLI flags always take precedence.

### Environment Variables

```bash
export PG_CONN_STRING="postgres://user:pass@localhost:5432/dbname"
pgqueue-dash
```

### CLI Flags (recommended)

```bash
pgqueue-dash \
  --dsn="postgres://user:pass@localhost:5432/task_queue?sslmode=disable" \
  --poll=2s
```

---

## CLI Arguments

| Flag     | Default | Description                      |
| -------- | ------- | -------------------------------- |
| `--dsn`  | `""`    | PostgreSQL connection string     |
| `--poll` | `5s`    | Refresh interval (minimum 500ms) |

