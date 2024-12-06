package btree

import (
	"errors"
	"log"
	"testing"
)

var (
	notfound = errors.New("item not found")
)

type InMemoryStore[K any, V any] struct {
	store []Node[K, int, V]
}

func (s *InMemoryStore[K, V]) get(ptr int) (*Node[K, int, V], error) {
	if ptr >= len(s.store) {
		return nil, notfound
	}

	return &s.store[ptr], nil
}

func (s *InMemoryStore[K, V]) Get(ptr int) (n Node[K, int, V], err error) {
	node, err := s.get(ptr)
	if err != nil {
		return
	}
	return *node, nil
}

func (s *InMemoryStore[K, V]) Update(ptr int, n Node[K, int, V]) error {
	_, err := s.get(ptr)
	if err != nil {
		return err
	}

	s.store[ptr] = n
	return nil
}

func (s *InMemoryStore[K, V]) Put(n Node[K, int, V]) (int, error) {
	s.store = append(s.store, n)
	return len(s.store) - 1, nil
}

func cmp(a, b rune) int {
	if a < b {
		return -1
	}
	if a == b {
		return 0
	}
	return +1
}

func printtree[K any, P any, V any](b *BTree[K, P, V]) {
	n := -1
	b.VisitLevelOrder(func(node Node[K, P, V]) bool {
		n++
		log.Printf("%v keys: %+v (ptrs: %+v)", n, node.Keys, node.Pointers)
		return true
	})
}

func TestBTree(t *testing.T) {
	store := InMemoryStore[rune, int]{}
	tree, err := New(&store, cmp, 3)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	alphabet := []rune("abcdefghijklmnopqrstuvwxyz")
	for i, r := range alphabet {
		// log.Println("==== tree dump ====")
		// printtree(tree)
		// log.Println("==== tree dump ====")
		// log.Println("-> inserting", r, i)

		if err := tree.Insert(r, i); err != nil {
			t.Fatalf("Failed to insert(%v, %v): %v", r, i, err)
		}
	}

	for i, r := range alphabet {
		if err := tree.Insert(r, i); err != ErrExists {
			t.Fatalf("insertion of (%v, %v) failed with unexpected error: %v", r, i, err)
		} else if err != ErrExists {
			t.Fatalf("insertion of (%v, %v) failed with unexpected succeeded", r, i)
		}
	}

	// log.Println("==== done; now querying ====")
	// printtree(tree)
	// log.Println("====")

	for i, r := range alphabet {
		v, found, err := tree.Find(r)
		if err != nil {
			t.Fatalf("Find(%v) unexpectedly failed", r)
		}
		if !found {
			t.Fatalf("Find(%v) unexpectedly not found", r)
		}
		if v != i {
			t.Fatalf("Find(%v) yielded %v, want %v", r, v, i)
		}
	}

	for i := len(alphabet) - 1; i >= 0; i-- {
		r := alphabet[i]
		v, found, err := tree.Find(r)
		if err != nil {
			t.Fatalf("Find(%v) unexpectedly failed", r)
		}
		if !found {
			t.Fatalf("Find(%v) unexpectedly not found", r)
		}
		if v != i {
			t.Fatalf("Find(%v) yielded %v, want %v", r, v, i)
		}
	}

	nonexist := 'A'
	v, found, err := tree.Find(nonexist)
	if err != nil {
		t.Fatalf("Find(%v) unexpectedly failed", nonexist)
	}
	if found {
		t.Fatalf("Find(%v) unexpectedly found %v", nonexist, v)
	}
}

func TestScanAll(t *testing.T) {
	store := InMemoryStore[rune, int]{}
	tree, err := New(&store, cmp, 3)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	alphabet := []rune("abcdefghijklmnopqrstuvwxyz")
	for i, r := range alphabet {
		if err := tree.Insert(r, i); err != nil {
			t.Fatalf("Failed to insert(%v, %v): %v", r, i, err)
		}
	}

	iter, err := tree.ScanAll()
	if err != nil {
		t.Fatalf("ScanAll failed: %v", err)
	}

	for i, r := range alphabet {
		if !iter.Next() {
			t.Fatalf("iterator stopped too early!")
		}
		k, v := iter.Current()
		if k != r {
			t.Errorf("Got key %v; want %v", k, r)
		}
		if v != i {
			t.Errorf("Got value %v; want %v", v, i)
		}
	}

	if iter.Next() {
		t.Fatalf("iterator could unexpectedly continue")
	}
}

func TestPersist(t *testing.T) {
	order := 3
	store := InMemoryStore[rune, int]{}
	tree1, err := New(&store, cmp, order)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	//alphabet := []rune("abcdefghijklmnopqrstuvwxyz")
	alphabet := []rune("abcdef")
	for i, r := range alphabet {
		if err := tree1.Insert(r, i); err != nil {
			t.Fatalf("Failed to insert(%v, %v): %v", r, i, err)
		}
	}

	store2 := InMemoryStore[rune, int]{}
	root, err := Persist(tree1, &store2)
	if err != nil {
		t.Fatalf("Failed to persist the tree: %v", err)
	}

	tree2 := FromStorage(root, &store2, cmp, order)
	// printtree(tree)
	// log.Println("===")
	// printtree(tree2)
	for i, r := range alphabet {
		v, found, err := tree2.Find(r)
		if err != nil {
			t.Fatalf("Find(%v) unexpectedly failed", r)
		}
		if !found {
			t.Fatalf("Find(%v) unexpectedly not found", r)
		}
		if v != i {
			t.Fatalf("Find(%v) yielded %v, want %v", r, v, i)
		}
	}

	nonexist := 'A'
	v, found, err := tree2.Find(nonexist)
	if err != nil {
		t.Fatalf("Find(%v) unexpectedly failed", nonexist)
	}
	if found {
		t.Fatalf("Find(%v) unexpectedly found %v", nonexist, v)
	}

	iter, err := tree2.ScanAll()
	if err != nil {
		t.Fatalf("ScanAll failed: %v", err)
	}

	for i, r := range alphabet {
		if !iter.Next() {
			t.Fatalf("iterator stopped too early!")
		}
		k, v := iter.Current()
		if k != r {
			t.Errorf("Got key %v; want %v", k, r)
		}
		if v != i {
			t.Errorf("Got value %v; want %v", v, i)
		}
	}

	if iter.Next() {
		t.Fatalf("iterator could unexpectedly continue")
	}
}
