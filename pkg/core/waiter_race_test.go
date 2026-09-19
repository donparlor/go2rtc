package core

import (
	"errors"
	"testing"
)

func TestWaiterConcurrentResultPublication(t *testing.T) {
	sentinel := errors.New("waiter result")

	for i := 0; i < 250; i++ {
		var w Waiter

		w.Add(1)

		result := make(chan error, 1)

		go func() {
			result <- w.Wait()
		}()

		go w.Done(sentinel)

		err := <-result

		if !errors.Is(err, sentinel) {
			t.Fatalf(
				"iteration %d: Wait returned %v; want sentinel",
				i,
				err,
			)
		}
	}
}
