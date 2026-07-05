// Command netpulse concurrently checks the health of multiple network
// endpoints using a bounded worker pool, and reports latency and status
// for each target.
//
// Design goals:
//   - Bounded concurrency: a fixed-size worker pool processes an arbitrary
//     number of targets without spawning unbounded goroutines.
//   - Cancellation propagation: a single context.Context governs per-request
//     timeouts and responds immediately to SIGINT/SIGTERM, unwinding all
//     in-flight workers cleanly.
//   - Deterministic output ordering: results are collected and sorted by
//     input order, not by completion order, so output is reproducible
//     across runs despite concurrent execution.
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

// target represents a single endpoint to probe.
type target struct {
	index int
	url   string
}

// result holds the outcome of probing a single target.
type result struct {
	index    int
	url      string
	status   int
	latency  time.Duration
	err      error
}

func main() {
	var (
		concurrency = flag.Int("c", 4, "number of concurrent workers")
		timeout     = flag.Duration("timeout", 5*time.Second, "per-request timeout")
		urlsFlag    = flag.String("urls", "", "comma-separated list of URLs to check")
	)
	flag.Parse()

	if strings.TrimSpace(*urlsFlag) == "" {
		fmt.Fprintln(os.Stderr, "netpulse: at least one URL is required, e.g. -urls=https://example.com,https://example.org")
		os.Exit(2)
	}
	rawURLs := strings.Split(*urlsFlag, ",")

	// Root context is cancelled on SIGINT/SIGTERM so all in-flight workers
	// unwind promptly instead of leaking goroutines or blocking process exit.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	targets := make([]target, 0, len(rawURLs))
	for i, u := range rawURLs {
		u = strings.TrimSpace(u)
		if u == "" {
			continue
		}
		targets = append(targets, target{index: i, url: u})
	}

	results := runPool(ctx, targets, *concurrency, *timeout)

	sort.Slice(results, func(i, j int) bool { return results[i].index < results[j].index })

	exitCode := 0
	for _, r := range results {
		if r.err != nil {
			fmt.Printf("%-45s  ERROR   %v\n", r.url, r.err)
			exitCode = 1
			continue
		}
		status := "OK"
		if r.status >= 400 {
			status = "FAIL"
			exitCode = 1
		}
		fmt.Printf("%-45s  %-6s  %4d  %8s\n", r.url, status, r.status, r.latency.Round(time.Millisecond))
	}
	os.Exit(exitCode)
}

// runPool distributes targets across a bounded set of workers and collects
// results via a dedicated results channel, closed once all workers finish.
func runPool(ctx context.Context, targets []target, concurrency int, timeout time.Duration) []result {
	if concurrency < 1 {
		concurrency = 1
	}

	jobs := make(chan target)
	resultsCh := make(chan result, len(targets))

	var wg sync.WaitGroup
	wg.Add(concurrency)
	for w := 0; w < concurrency; w++ {
		go func() {
			defer wg.Done()
			worker(ctx, jobs, resultsCh, timeout)
		}()
	}

	go func() {
		defer close(jobs)
		for _, t := range targets {
			select {
			case jobs <- t:
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	collected := make([]result, 0, len(targets))
	for r := range resultsCh {
		collected = append(collected, r)
	}
	return collected
}

// worker pulls targets from jobs until the channel is closed or ctx is
// cancelled, probing each one and publishing a result.
func worker(ctx context.Context, jobs <-chan target, out chan<- result, timeout time.Duration) {
	client := &http.Client{}
	for {
		select {
		case t, ok := <-jobs:
			if !ok {
				return
			}
			out <- probe(ctx, client, t, timeout)
		case <-ctx.Done():
			return
		}
	}
}

// probe issues a single GET request against t.url, bounded by timeout, and
// measures wall clock latency.
func probe(ctx context.Context, client *http.Client, t target, timeout time.Duration) result {
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, t.url, nil)
	if err != nil {
		return result{index: t.index, url: t.url, err: err}
	}

	start := time.Now()
	resp, err := client.Do(req)
	elapsed := time.Since(start)
	if err != nil {
		return result{index: t.index, url: t.url, err: err, latency: elapsed}
	}
	defer resp.Body.Close()

	return result{index: t.index, url: t.url, status: resp.StatusCode, latency: elapsed}
}
