package executor

import (
	"context"
	"github.com/pgvanniekerk/ezPool/pool"
	"github.com/pgvanniekerk/ezWorker/worker"
)

// New initializes and returns an Executor with the specified input, output, and error channels,
// worker factory, and context. It also creates a fixed-size worker pool and populates it with
// workers using the provided worker factory. Returns the initialized Executor and an error if
// the worker creation or pool population fails.
func New[I, O any](
	inputChannel <-chan I,
	outputChannel chan<- O,
	errorChannel chan<- error,
	cancelContext context.Context,
	workerFactory worker.Factory[I, O],
	workerCount uint16,
) (Executor[I, O], error) {

	// Create a new instance of executor that will be populated and returned.
	e := &executor[I, O]{
		inputChannel:  inputChannel,
		outputChannel: outputChannel,
		errorChannel:  errorChannel,
		cancelContext: cancelContext,
		workerFactory: workerFactory,
		workerPool:    pool.NewFixedSizedPool[worker.Worker[I, O]](uint32(workerCount)),
	}

	// Populate the worker pool using worker instances created by workerFactory.
	for i := 0; i < int(workerCount); i++ {

		// Create a new worker instance.
		wrkr, err := e.workerFactory.Create()
		if err != nil {
			return nil, err
		}

		// Add the instance to the worker pool.
		err = e.workerPool.Put(wrkr)
		if err != nil {
			return nil, err
		}
	}

	return e, nil
}
