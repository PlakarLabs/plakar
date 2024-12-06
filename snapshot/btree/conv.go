package btree

func persist[K any, PA any, PB any, V any](b *BTree[K, PA, V], store Storer[K, PB, V], node *Node[K, PA, V], lastptr **PB) (PB, error) {
	var ptrs []PB
	var zero PB

	for i := len(node.Pointers) - 1; i >= 0; i-- {
		var child Node[K, PA, V]
		child, err := b.store.Get(node.Pointers[i])
		if err != nil {
			return zero, err
		}

		ptr, err := persist(b, store, &child, lastptr)
		if err != nil {
			return zero, err
		}
		if child.isleaf() {
			*lastptr = new(PB)
			**lastptr = ptr
		}
		ptrs = append(ptrs, ptr)
	}

	// reverse pointers
	for i := len(ptrs)/2 - 1; i >= 0; i-- {
		opp := len(ptrs) - 1 - i
		ptrs[i], ptrs[opp] = ptrs[opp], ptrs[i]
	}

	newnode := Node[K, PB, V]{
		Keys:     node.Keys,
		Values:   node.Values,
		Pointers: ptrs,
	}
	if node.isleaf() && *lastptr != nil {
		newnode.Next = *lastptr
	}
	return store.Put(newnode)
}

func Persist[K any, PA any, PB any, V any](b *BTree[K, PA, V], store Storer[K, PB, V]) (ptr PB, err error) {
	root, err := b.store.Get(b.root)
	if err != nil {
		return
	}

	var lastptr *PB
	return persist(b, store, &root, &lastptr)
}
