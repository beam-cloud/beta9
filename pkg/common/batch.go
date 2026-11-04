package common

import "time"

// RunBatchQueue collects bounded one-millisecond windows and dispatches them
// serially. Closing the queue drains its remaining requests before returning.
func RunBatchQueue[T any](queue <-chan T, limit int, dispatch func([]T)) {
	for first := range queue {
		batch := []T{first}
		timer := time.NewTimer(time.Millisecond)
	collect:
		for len(batch) < max(limit, 1) {
			select {
			case request, open := <-queue:
				if !open {
					timer.Stop()
					dispatch(batch)
					return
				}
				batch = append(batch, request)
			case <-timer.C:
				break collect
			}
		}
		timer.Stop()
		dispatch(batch)
	}
}
