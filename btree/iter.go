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

// ScanAll returns an iterator that visits all the values from the
// smaller one onwards.
func (b *BTree[K, P, V]) ScanAll() (Iterator[K, V], error) {
	ptr := b.Root

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
		current: n,
		idx:     -1,
	}, nil
}

// ScanFrom returns an iterator that visits all the values starting
// from the given key, or the first key larger than the given one,
// onwards.
func (b *BTree[K, P, V]) ScanFrom(key K, cmp func(K, K) int) (Iterator[K, V], error) {
	if cmp == nil {
		cmp = b.compare
	}

	node, _, err := b.findleaf(key, cmp)
	if err != nil {
		return nil, err
	}

	var (
		idx int
		found bool
	)
	for idx = range node.Keys {
		if cmp(key, node.Keys[idx]) <= 0 {
			found = true
			break
		}
	}
	if !found {
		if node.Next == nil {
			idx++ // key not found, make an empty iterator
		} else {
			node, err = b.store.Get(*node.Next)
			if err != nil {
				return nil, err
			}
			idx = 0
		}
	}

	idx-- // forwardIter.Next() will bump this
	return &forwardIter[K, P, V]{
		b:       b,
		current: &node,
		idx:     idx,
	}, nil
}

func (b *BTree[K, P, V]) VisitLevelOrder(cb func(Node[K, P, V]) bool) error {
	stack := []P{b.Root}

	for {
		if len(stack) == 0 {
			return nil
		}
		ptr := stack[0]
		stack = stack[1:]

		node, err := b.store.Get(ptr)
		if err != nil {
			return err
		}

		if !cb(node) {
			return nil
		}

		stack = append(stack, node.Pointers...)
	}
}
