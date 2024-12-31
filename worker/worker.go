package worker

import "context"

type Worker[I, O any] interface {
	Execute(context.Context, I) (O, error)
}
