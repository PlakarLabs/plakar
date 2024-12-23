package btree

import (
	"errors"
	"fmt"
	"log"
	"slices"
	"strings"
)

var (
	ErrExists = errors.New("Item already exists")
)

type Storer[K any, P any, V any] interface {
	// Get returns the node pointed by P.  The pointer is one
	// previously returned by the Put method.
	Get(P) (Node[K, P, V], error)
	// Updates in-place the node pointed by P.
	Update(P, Node[K, P, V]) error
	// Put saves a new node and returns its address, or an error.
	Put(Node[K, P, V]) (P, error)
}

type Node[K any, P any, V any] struct {
	// An intermediate node has only Keys and Pointers, while
	// leaves have only keys and values and optionally a next
	// pointer.
	//
	// invariant: len(Pointers) == len(Keys) + 1 in intermediate nodes
	// invariant: len(Values)   == len(Keys)     in leaf nodes
	Keys     []K `msgpack:"keys"`
	Pointers []P `msgpack:"pointers"`
	Values   []V `msgpack:"values"`
	Next     *P  `msgpack:"next,omitempty"`
}

// BTree implements a B+tree.  K is the type for the key, V for the
// value stored, and P is a pointer type: it could be a disk sector,
// a checksum in a packfile, or a key in a leveldb cache.  or more.
type BTree[K any, P any, V any] struct {
	Order   int
	Root    P
	store   Storer[K, P, V]
	compare func(K, K) int
}

// New returns a new, empty tree.
func New[K any, P any, V any](store Storer[K, P, V], compare func(K, K) int, order int) (*BTree[K, P, V], error) {
	root := Node[K, P, V]{}
	ptr, err := store.Put(root)
	if err != nil {
		return nil, err
	}

	return &BTree[K, P, V]{
		Order:   order,
		Root:    ptr,
		store:   store,
		compare: compare,
	}, nil
}

// FromStorage returns a btree from the given storage.  The root must
// exist, eventually empty, i.e. it should be a tree previously
// created via New().
func FromStorage[K any, P any, V any](root P, store Storer[K, P, V], compare func(K, K) int, order int) *BTree[K, P, V] {
	return &BTree[K, P, V]{
		Order:   order,
		Root:    root,
		store:   store,
		compare: compare,
	}
}

func (n *Node[K, P, V]) isleaf() bool {
	return len(n.Pointers) == 0
}

func (b *BTree[K, P, V]) findleaf(key K) (node Node[K, P, V], path []P, err error) {
	ptr := b.Root

	for {
		path = append(path, ptr)
		node, err = b.store.Get(ptr)
		if err != nil {
			return
		}
		//log.Println("findleaf: trace: loaded", node)

		if node.isleaf() {
			return
		}

		idx, found := slices.BinarySearchFunc(node.Keys, key, b.compare)
		//log.Printf("in %v; binary search was %d %v", node.Keys, idx, found)
		if found {
			idx++
		}
		if idx < len(node.Keys) {
			ptr = node.Pointers[idx]
		} else {
			ptr = node.Pointers[len(node.Keys)]
		}
	}
}

func (b *BTree[K, P, V]) Find(key K) (val V, found bool, err error) {
	//log.Println("Find", key)

	leaf, _, err := b.findleaf(key)
	if err != nil {
		return
	}

	//log.Println("found leaf", leaf)

	val, found = leaf.find(key, b.compare)
	return val, found, nil
}

func (n *Node[K, P, V]) find(key K, cmp func(K, K) int) (val V, found bool) {
	idx, found := slices.BinarySearchFunc(n.Keys, key, cmp)
	if found {
		return n.Values[idx], true
	}
	return val, false
}

func (n *Node[K, P, V]) insertAt(idx int, key K, val V, b *BTree[K, P, V]) {
	n.Keys = slices.Insert(n.Keys, idx, key)
	n.Values = slices.Insert(n.Values, idx, val)

	var previous *K
	for i := range n.Keys {
		curr := n.Keys[i]
		if previous != nil {
			if b.compare(*previous, curr) != -1 {
				panic(fmt.Sprintf("inversion: %v vs %v", *previous, key))
			}
		}
		previous = &curr
	}
}

func (n *Node[K, P, V]) insertInternal(idx int, key K, ptr P, cmp func(K, K) int) {
	// Pointers and Keys have different cardinalities, but to
	// decide whether to append or insert in Pointers we need
	// to consider the length of the keys.
	log.Printf("before %v / %v ; idx=%v key=%v, ptr=%v", n.Keys, n.Pointers, idx, key, ptr)
	if idx >= len(n.Keys) {
		n.Keys = append(n.Keys, key)
		n.Pointers = append(n.Pointers, ptr)
	} else {
		n.Keys = slices.Insert(n.Keys, idx, key)
		n.Pointers = slices.Insert(n.Pointers, idx + 1, ptr)
	}

	log.Printf("after %v / %v", n.Keys, n.Pointers)

	var previous *K
	for i := range n.Keys {
		curr := n.Keys[i]
		if previous != nil {
			if cmp(*previous, curr) != -1 {
				panic(fmt.Sprintf("inversion: %v vs %v", *previous, key))
			}
		}
		previous = &curr
	}
}

func (b *BTree[K, P, V]) findsplit(key K, node *Node[K, P, V]) (int, bool) {
	return slices.BinarySearchFunc(node.Keys, key, b.compare)
}

func (n *Node[K, P, V]) split() Node[K, P, V] {
	cutoff := (len(n.Keys) + 1) / 2 - 1
	if cutoff == 0 {
		cutoff = 1
	}
	//log.Println("cutoff is", cutoff)
	
	new := Node[K, P, V]{
		Keys: make([]K, len(n.Keys)-cutoff),
		Next: n.Next,
	}

	copy(new.Keys, n.Keys[cutoff:])
	n.Keys = n.Keys[:cutoff]

	if n.isleaf() {
		new.Values = make([]V, len(n.Values)-cutoff)
		copy(new.Values, n.Values[cutoff:])
		n.Values = n.Values[:cutoff]
	} else {
		cutoff++
		new.Pointers = make([]P, len(n.Pointers)-cutoff)
		copy(new.Pointers, n.Pointers[cutoff:])
		n.Pointers = n.Pointers[:cutoff]
	}
	//log.Println("split: trace: nodes are", n, new)
	return new
}

func (b *BTree[K, P, V]) insert(key K, val V, overwrite bool) error {
	node, path, err := b.findleaf(key)
	if err != nil {
		return err
	}

	ptr := path[len(path)-1]
	path = path[:len(path)-1]

	idx, found := b.findsplit(key, &node)
	if found {
		if overwrite {
			//log.Println("overwriting", key, node.Keys[idx])
			node.Values[idx] = val
			return b.store.Update(ptr, node)
		}
		//log.Println("insert:", key, "is already in the btree")
		return ErrExists
	}

	if len(node.Keys) < b.Order-1 {
		node.insertAt(idx, key, val, b)
		return b.store.Update(ptr, node)
	}

	node.insertAt(idx, key, val, b)
	//log.Println("inserted new element before splitting:", node.Keys)
	new := node.split()
	//log.Println("split into", node.Keys, new.Keys)

	// log.Println("before splitting node is", node.Keys)
	// new := node.split()
	// log.Println("splitted into", node.Keys, new.Keys)
	// if idx < len(node.Keys) {
	// 	node.insertAt(idx, key, val, b)
	// } else {
	// 	idx -= len(node.Keys)
	// 	new.insertAt(idx, key, val, b)
	// }
	// new.Next = node.Next

	newptr, err := b.store.Put(new)
	if err != nil {
		return err
	}
	node.Next = &newptr
	if err := b.store.Update(ptr, node); err != nil {
		return err
	}

	log.Printf("after inserting %v; got %+v and %+v; bubbling up (%v, %v)", key,
		node.Keys, new.Keys, new.Keys[0], newptr)
	key = new.Keys[0]
	return b.insertUpwards(key, newptr, path)
}

func (b *BTree[K, P, V]) Dot() string {
	w := strings.Builder{}

	b.VisitLevelOrder2(func(n Node[K, P, V], ptr P) bool {
		fmt.Fprintf(&w, "%v [label=%q]\n", ptr, fmt.Sprintf("%v %v", ptr, n.Keys))
		for _, cptr := range n.Pointers {
			fmt.Fprintf(&w, "%v -> %v\n", ptr, cptr)
		}
		// if n.Next != nil {
		// 	fmt.Fprintf(&w, "%v -> %v\n", ptr, *n.Next)
		// }
		return true
	})

	return w.String()
}

var run uint64 = 0
func (b *BTree[K, P, V]) Validate() {
	iter, err := b.ScanAll()
	if err != nil {
		panic(err)
	}

	// log.Printf("graph is:\n%s\n", b.Dot())

	run++
	var previous *K
	for iter.Next() {
		k, _ := iter.Current()
		//log.Println(run, "viewing", k)
		
		if previous != nil {
			if b.compare(*previous, k) != -1 {
				panic(fmt.Sprintf("validate: inversion: %v vs %v", *previous, k))
			}
		}
		previous = &k
	}

	if err := iter.Err(); err != nil {
		panic(err)
	}
}

func (b *BTree[K, P, V]) Insert(key K, val V) error {
	//log.Println("inserting", key)
	if err := b.insert(key, val, false); err != nil {
		return err
	}
	b.Validate()
	//log.Println("inserted", key)
	return nil
}

func (b *BTree[K, P, V]) Update(key K, val V) error {
	if err := b.insert(key, val, true); err != nil {
		return err
	}
	b.Validate()
	return nil
}

func (b *BTree[K, P, V]) insertUpwards(key K, ptr P, path []P) error {
	log.Printf("insertUpwards of (%v, %v) with path %v", key, ptr, path)

	for i := len(path) - 1; i >= 0; i-- {
		node, err := b.store.Get(path[i])
		if err != nil {
			return err
		}

		idx, found := b.findsplit(key, &node)
		if found {
			panic("broken invariant: duplicate key in intermediate node")
		}

		if len(node.Keys) < b.Order-1 {
			log.Printf("insert at %v without splitting %v (@%v)", idx, node.Keys, path[i])
			node.insertInternal(idx, key, ptr, b.compare)
			return b.store.Update(path[i], node)
		}

		node.insertInternal(idx, key, ptr, b.compare)
		log.Printf("inserted at %v in %v", idx, node.Keys)

		new := node.split()
		// if idx < len(node.Keys) {
		// 	node.insertInternal(idx, key, ptr, b.compare)
		// } else {
		// 	idx -= len(node.Keys)
		// 	new.insertInternal(idx, key, ptr, b.compare)
		// }
		key = new.Keys[0]
		keys := make([]K, len(new.Keys)-1)
		copy(keys, new.Keys[1:])
		new.Keys = keys
		//new.Keys = new.Keys[1:]
		//log.Println("split internal into", node, new)
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
		Pointers: []P{b.Root, ptr},
	}
	rootptr, err := b.store.Put(newroot)
	if err != nil {
		return err
	}
	b.Root = rootptr
	return nil
}
