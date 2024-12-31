package executor

import (
	"context"
	"fmt"
	"github.com/pgvanniekerk/ezPool/pool"
	"github.com/pgvanniekerk/ezWorker/worker"
)

// Executor is used to asynchronously execute tasks with input type I and output type O,
// making use of a worker pool to limit resource usage.
// Run executes the main task logic defined by the implementation.
// Teardown performs cleanup operations and returns an error if the process fails.
type Executor[I, O any] interface {

	// Run executes the main task logic as defined by the specific implementation
	// of the Executor interface.
	Run()

	// Teardown performs resource cleanup and shutdown tasks, returning an error
	// if any issues occur during the process.
	Teardown() error
}

// executor is a type that manages concurrent task execution using a worker pool
// and channels for input, output, and errors.
type executor[I, O any] struct {

	// inputChannel receives input messages of type I for the executor
	// to process.
	inputChannel <-chan I

	// outputChannel is a channel where processed output messages of
	// type O are sent from the executor.
	outputChannel chan<- O

	// errorChannel is used to send error messages encountered during the
	// processing of input messages.
	errorChannel chan<- error

	// workerPool is a pool of reusable workers responsible for processing
	// tasks of type I to produce results of type O.
	workerPool pool.Pool[worker.Worker[I, O]]

	// cancelContext is a context used to signal cancellation and manage the
	// lifecycle of the executor's processing.
	cancelContext context.Context

	// workerFactory defines the factory used to create and destroy workers for
	// processing tasks of type I to produce results of type O.
	workerFactory worker.Factory[I, O]
}

// Run starts the task execution loop, processing inputs and managing workers until
// cancellation or input closure. Run is executed asynchronously. Any critical errors
// will cause a panic.
func (e *executor[I, O]) Run() {

	// Execute logic asynchronously in a separate goroutine.
	go func(e *executor[I, O]) {

		// Continuously loop to listen for incoming messages
		// or cancellation signals.
		for {
			select {

			// Handle cancellation of task execution
			case <-e.cancelContext.Done():
				e.errorChannel <- e.cancelContext.Err()
				err := e.Teardown()
				if err != nil {
					panic(fmt.Errorf("failed to teardown executor: %w", err))
				}
				return

			// Handle incoming messages
			case msg, closed := <-e.inputChannel:

				// Check if the channel has been closed; If so, cleanup
				// resources and return normally.
				if closed {
					err := e.Teardown()
					if err != nil {
						panic(fmt.Errorf("failed to teardown executor: %w", err))
					}
					return
				}

				// Get the next available worker from the worker pool.
				wrkr := e.workerPool.Get()

				// Asynchronously execute processing of the message read
				// from the input channel.
				go func(msg I, w worker.Worker[I, O], e *executor[I, O]) {

					// Defer returning the worker back to the pool after
					// finishing task execution.
					defer func() {
						err := e.workerPool.Put(w)

						// If an error occurs during this step, Teardown resources and
						// panic. This error should in theory never happen.
						if err != nil {
							putErr := e.Teardown()
							if putErr != nil {
								panic(fmt.Errorf("failed to teardown executor: %w", putErr))
							}
							panic(fmt.Errorf("failed to put worker back into pool: %w", putErr))
						}
					}()

					// Execute the task providing the message read from
					// the input channel. Should an error occur, send the
					// error into errorChannel to notify the user and allow
					// them to handle the error gracefully if required. If
					// no error occurs, send the output into output channel
					// allowing the user to perform any required logic.
					output, err := w.Execute(e.cancelContext, msg)
					if err != nil {
						e.errorChannel <- err
						return
					}
					e.outputChannel <- output

				}(msg, wrkr, e) // Pass all data into the async routine, negating the need for a closure.
			}
		}
	}(e) // Pass all data into the async routine, negating the need for a closure.
}

// Teardown releases all resources used by the executor, including workers
// and the worker pool. It panics on errors.
func (e *executor[I, O]) Teardown() error {

	// Iterate through all available workers to release any
	// required resources.
	for i := uint32(0); i < e.workerPool.Avail(); i++ {

		// Get the next available working from the pool.
		wrkr := e.workerPool.Get()

		// Safely release any resources used by the working.
		err := e.workerFactory.Destroy(wrkr)
		if err != nil {
			return fmt.Errorf("failed to destroy worker: %w", err)
		}
	}

	// Release the resources acquired by the pool.
	err := e.workerPool.Teardown()
	if err != nil {
		return fmt.Errorf("failed to teardown worker pool: %w", err)
	}

	return nil
}
