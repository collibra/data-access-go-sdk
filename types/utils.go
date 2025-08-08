package types

type ListItem[T any] struct {
	item *T
	err  error
}

func NewListItemItem[T any](item *T) ListItem[T] {
	return ListItem[T]{item: item}
}

func NewListItemError[T any](err error) ListItem[T] {
	return ListItem[T]{err: err}
}

func (l *ListItem[T]) HasError() bool {
	return l.err != nil
}

func (l *ListItem[T]) GetError() error {
	return l.err
}

func (l *ListItem[T]) GetItem() *T {
	return l.item
}

func (l *ListItem[T]) MustGetItem() T {
	if l.item == nil {
		panic("item was nil")
	}

	return *l.item
}
