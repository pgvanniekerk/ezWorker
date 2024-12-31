package worker

type Factory[I, O any] interface {
	Create() (Worker[I, O], error)
	Destroy(Worker[I, O]) error
}
