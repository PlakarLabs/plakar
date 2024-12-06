package btree

import (
	"errors"
)

var (
	ErrExists = errors.New("Item already exists")
)

type Storer[K any, P any, V any] interface {
	Get(P) (Node[K, P, V], error)
	Update(P, Node[K, P, V]) error
	Put(Node[K, P, V]) (P, error)
}

type Node[K any, P any, V any] struct {
	// An intermediate node has only Keys and Pointers, while
	// leaves have only keys and values.
	Keys     []K
	Pointers []P // invariant: len(Pointers) == len(Keys) + 1 in intermediate nodes
	Values   []V // invariant: len(Values) == len(Keys)       in leaf nodes
}

// BTree implements a B+tree.  K is the type for the key, V for the
// value stored, and P is a pointer type: it could be a disk sector,
// a checksum in a packfile, or a key in a leveldb cache.  or more.
type BTree[K any, P any, V any] struct {
	order   int
	root    P
	store   Storer[K, P, V]
	compare func(K, K) int
}

func New[K any, P any, V any](store Storer[K, P, V], compare func(K, K) int, order int) (*BTree[K, P, V], error) {
	root := Node[K, P, V]{}
	ptr, err := store.Put(root)
	if err != nil {
		return nil, err
	}

	return &BTree[K, P, V]{
		order:   order,
		root:    ptr,
		store:   store,
		compare: compare,
	}, nil
}

func (n *Node[K, P, V]) isleaf() bool {
	return len(n.Pointers) == 0
}

func (b *BTree[K, P, V]) findleaf(key K) (node Node[K, P, V], path []P, err error) {
	ptr := b.root

outer:
	for {
		path = append(path, ptr)
		node, err = b.store.Get(ptr)
		if err != nil {
			return
		}

		if node.isleaf() {
			return
		}

		for i := range node.Keys {
			if b.compare(key, node.Keys[i]) < 0 {
				ptr = node.Pointers[i]
				continue outer
			}
		}
		ptr = node.Pointers[len(node.Keys)]
	}
}

func (b *BTree[K, P, V]) Find(key K) (val V, found bool, err error) {
	leaf, _, err := b.findleaf(key)
	if err != nil {
		return
	}

	val, found = leaf.find(key, b.compare)
	return val, found, nil
}

func (n *Node[K, P, V]) find(key K, cmp func(K, K) int) (val V, found bool) {
	for i := range n.Keys {
		if cmp(key, n.Keys[i]) == 0 {
			return n.Values[i], true
		}
	}
	return
}

func (n *Node[K, P, V]) insertAt(idx int, key K, val V) {
	if idx >= len(n.Keys) {
		n.Keys = append(n.Keys, key)
		n.Values = append(n.Values, val)
		return
	}

	var ks []K
	copy(ks, n.Keys[:idx])
	ks = append(ks, key)
	ks = append(ks, n.Keys[idx:]...)
	n.Keys = ks

	var vs []V
	copy(vs, n.Values[idx:])
	vs = append(vs, val)
	vs = append(vs, n.Values[idx:]...)
	n.Values = vs
}

func (n *Node[K, P, V]) insertInternal(idx int, key K, ptr P) {
	if idx >= len(n.Keys) {
		n.Keys = append(n.Keys, key)
		n.Pointers = append(n.Pointers, ptr)
		return
	}

	var ks []K
	ks = append(ks, n.Keys[:idx]...)
	ks = append(ks, key)
	ks = append(ks, n.Keys[idx:]...)
	n.Keys = ks

	var ps []P
	ps = append(ps, n.Pointers[:idx]...)
	ps = append(ps, ptr)
	ps = append(ps, n.Pointers[idx:]...)
	n.Pointers = ps
}

func (b *BTree[K, P, V]) findsplit(key K, node *Node[K, P, V]) (idx int, found bool) {
	for idx = range node.Keys {
		r := b.compare(key, node.Keys[idx])
		if r <= 0 {
			found = r == 0
			break
		}
	}
	idx++
	return
}

func (n *Node[K, P, V]) split() (new Node[K, P, V]) {
	cutoff := (len(n.Keys) + 1) / 2
	new.Keys = n.Keys[cutoff:]
	n.Keys = n.Keys[:cutoff]

	if n.isleaf() {
		new.Values = n.Values[cutoff:]
		n.Values = n.Values[:cutoff]
	} else {
		cutoff++
		new.Pointers = n.Pointers[cutoff:]
		n.Pointers = n.Pointers[:cutoff]
	}
	return
}

func (b *BTree[K, P, V]) Insert(key K, val V) error {
	node, path, err := b.findleaf(key)
	if err != nil {
		return err
	}

	idx, found := b.findsplit(key, &node)
	if found {
		return ErrExists
	}

	ptr := path[len(path)-1]
	if len(node.Keys) < b.order-1 {
		node.insertAt(idx, key, val)
		return b.store.Update(ptr, node)
	}

	new := node.split()
	if idx < len(node.Keys) {
		idx = 0
	} else {
		idx -= len(node.Keys)
	}
	new.insertAt(idx, key, val)

	newptr, err := b.store.Put(new)
	if err != nil {
		return err
	}
	if err := b.store.Update(ptr, node); err != nil {
		return err
	}

	key = new.Keys[0]
	return b.insertUpwards(key, newptr, path[:len(path)-1])
}

func (b *BTree[K, P, V]) insertUpwards(key K, ptr P, path []P) error {
	for i := len(path) - 1; i >= 0; i-- {
		node, err := b.store.Get(path[i])
		if err != nil {
			return err
		}

		idx, found := b.findsplit(key, &node)
		if found {
			panic("broken invariant: duplicate key in intermediate node")
		}

		if len(node.Keys) < b.order-1 {
			node.insertInternal(idx, key, ptr)
			return b.store.Update(path[i], node)
		}

		new := node.split()
		if idx < len(node.Keys) {
			idx = 0
		} else {
			idx -= len(node.Keys)
		}
		new.insertInternal(idx, key, ptr)
		key = new.Keys[0]
		new.Keys = new.Keys[1:]
		ptr, err = b.store.Put(new)
		if err != nil {
			return err
		}
		if err := b.store.Update(path[i], node); err != nil {
			return err
		}
	}

	// reached the root, growing the tree
	newroot := Node[K, P, V]{
		Keys:     []K{key},
		Pointers: []P{b.root, ptr},
	}
	rootptr, err := b.store.Put(newroot)
	if err != nil {
		return err
	}
	b.root = rootptr
	return nil
}
