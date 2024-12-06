package btree

type Iterator[K any, V any] interface {
	Next() bool
	Current() (K, V)
	Err() error
}

type forwardIter[K any, P any, V any] struct {
	b       *BTree[K, P, V]
	current *Node[K, P, V]
	err     error
	idx     int
}

func (fit *forwardIter[K, P, V]) Next() bool {
	if fit.err != nil {
		return false
	}

	fit.idx++
	if fit.idx < len(fit.current.Values) {
		return true
	}

	if fit.current.Next == nil {
		return false
	}

	fit.idx = 0
	next, err := fit.b.store.Get(*fit.current.Next)
	if err != nil {
		fit.err = err
		return false
	}

	fit.current = &next
	return true
}

func (fit *forwardIter[K, P, V]) Current() (K, V) {
	return fit.current.Keys[fit.idx], fit.current.Values[fit.idx]
}

func (fit *forwardIter[K, P, V]) Err() error {
	return fit.err
}

func (b *BTree[K, P, V]) ScanAll() (Iterator[K, V], error) {
	ptr := b.root

	var n *Node[K, P, V]
	for {
		node, err := b.store.Get(ptr)
		if err != nil {
			return nil, err
		}

		if node.isleaf() {
			n = &node
			break
		}
		ptr = node.Pointers[0]
	}

	return &forwardIter[K, P, V]{
		b:       b,
		err:     nil,
		current: n,
		idx:     -1,
	}, nil
}
