# pgqueue-dash 🐘

A high-performance Terminal User Interface (TUI) for monitoring the `pgqueue` task queue.

## Features
- **Live Stats**: Real-time worker connection counts and task distribution.
- **Search & Filter**: Case-insensitive filtering of task types and errors.
- **Pagination**: Efficiently browse millions of tasks using `LIMIT/OFFSET`.
- **Introspection**: View full JSON payloads and error stacks in a scrollable modal.
- **Actionable**: Manually retry failed tasks with a single keypress.

## Installation
```bash
go install github.com/i-christian/pgqueue/cmd/pgqueue-dash@latest

```

## Shortcuts

| Key | Action |
| --- | --- |
| `Tab` | Toggle between Overview and Task List |
| `/` | Start filtering tasks |
| `n` | Next page of tasks |
| `p` | Previous page of tasks |
| `Enter` | View task details |
| `r` | Retry selected failed task |
| `q` | Quit |

## Configuration
The dashboard can be configured via environment variables or CLI flags. Flags take precedence.

#### Using Environment Variables
```bash
export PG_CONN_STRING="postgres://user:pass@localhost:5432/dbname"

pgqueue-dash

```

#### Using Flags (Best for multiple projects)
```
  pgqueue-dash --dsn="postgres://myuser:mypass@localhost:5432/task_queue?sslmode=disable --poll=2s"
```

## CLI Arguments
| Flag | Default | Description |
| --- | --- | --- |
| `--dsn` | `""` | PostgreSQL connection string. |
| `--poll` | `5s` | Refresh rate (minimum 500ms). |
