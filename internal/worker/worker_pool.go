package worker

import (
	"context"
	"sync"
)

// Job is a unit of work executed by workers.
type Job func(ctx context.Context) error

// Pool executes jobs with bounded concurrency.
type Pool struct {
	workers int
}

func NewPool(workers int) *Pool {
	if workers <= 0 {
		workers = 1
	}
	return &Pool{workers: workers}
}

// Run executes all jobs and returns the first error encountered (if any).
func (p *Pool) Run(ctx context.Context, jobs []Job) error {
	if len(jobs) == 0 {
		return nil
	}

	jobCh := make(chan Job)
	errCh := make(chan error, len(jobs))
	var wg sync.WaitGroup

	for i := 0; i < p.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobCh {
				if err := job(ctx); err != nil {
					errCh <- err
				}
			}
		}()
	}

	for _, j := range jobs {
		select {
		case <-ctx.Done():
			break
		case jobCh <- j:
		}
	}
	close(jobCh)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}
