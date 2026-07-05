# netpulse

A small, dependency free command line tool that concurrently checks the
health of multiple HTTP endpoints using a bounded worker pool.

Written to demonstrate idiomatic Go concurrency patterns in isolation,
independent of any larger system: worker pools, context based cancellation,
and graceful shutdown.

## Why this exists

Most "concurrency demo" snippets either spawn an unbounded goroutine per
item, which does not scale, or ignore cancellation entirely, which leaks
goroutines when a caller gives up early. `netpulse` addresses both:

* **Bounded concurrency.** A fixed number of worker goroutines pull from a
  shared jobs channel, so checking 10,000 URLs with `-c=8` never spawns more
  than 8 concurrent HTTP requests.
* **Cancellation propagation.** A single `context.Context`, cancelled on
  `SIGINT`/`SIGTERM`, governs both the job dispatcher and every in-flight
  request. Ctrl+C unwinds the whole pool immediately instead of waiting for
  outstanding requests to time out naturally.
* **Deterministic output.** Workers complete in whatever order the network
  returns them, but results are sorted back into input order before
  printing, so output is reproducible across runs.

## Usage

```bash
go build -o netpulse .

./netpulse -urls=https://example.com,https://example.org -c=4 -timeout=5s
```

```
https://example.com                             OK       200      82ms
https://example.org                             OK       200      91ms
```

Flags:

| Flag        | Default | Description                              |
|-------------|---------|------------------------------------------|
| `-urls`     | (none)  | Comma separated list of URLs to check    |
| `-c`        | 4       | Number of concurrent workers             |
| `-timeout`  | 5s      | Per request timeout                      |

Exit code is `0` if every endpoint returned a non error status, `1`
otherwise, making it usable directly in CI health check steps.

## Architecture

```
main() ─┬─> dispatcher goroutine: feeds targets into jobs channel
         │                        (stops early if ctx is cancelled)
         │
         ├─> worker goroutine × N: pulls from jobs, probes URL,
         │                          publishes to results channel
         │
         └─> collector: drains results channel until all workers
                         signal completion via sync.WaitGroup
```

The dispatcher, workers, and collector run concurrently and communicate
exclusively through channels and a shared `context.Context`; no shared
mutable state is accessed outside of channel operations, which is why the
build is verified clean under Go's `-race` detector (see below).

## Verification

```bash
go vet ./...          # static analysis, clean
go build -race -o netpulse_race .
./netpulse_race -urls=... # exercised under the race detector, no warnings
```

## Scope

This is intentionally a standalone utility with no relation to any other
project. It exists solely to demonstrate concurrency design in Go.
