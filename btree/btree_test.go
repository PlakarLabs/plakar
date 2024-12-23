package btree

import (
	"math/rand"
	"errors"
	"log"
	"strings"
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
	log.Printf("store: updating @ %v; item is %v; %v", ptr, n.Keys, n.Pointers)

	nn := Node[K, int, V]{
		Keys: make([]K, len(n.Keys)),
		Pointers: make([]int, len(n.Pointers)),
		Values: make([]V, len(n.Values)),
		Next: n.Next,
	}
	copy(nn.Keys, n.Keys)
	copy(nn.Pointers, n.Pointers)
	copy(nn.Values, n.Values)

	s.store[ptr] = nn
	return nil
}

func (s *InMemoryStore[K, V]) Put(n Node[K, int, V]) (int, error) {
	nn := Node[K, int, V]{
		Keys: make([]K, len(n.Keys)),
		Pointers: make([]int, len(n.Pointers)),
		Values: make([]V, len(n.Values)),
		Next: n.Next,
	}
	copy(nn.Keys, n.Keys)
	copy(nn.Pointers, n.Pointers)
	copy(nn.Values, n.Values)

	log.Printf("store: adding new node @ %v: %v", len(s.store), n)
	s.store = append(s.store, nn)
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

func TestInsert(t *testing.T) {
	store := InMemoryStore[string, int]{}
	tree, err := New(&store, strings.Compare, 30)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	items := []string{"e", "z", "a", "b", "a", "a", "b", "b", "a", "c", "d"}
	for i, r := range items {
		// log.Println("==== tree dump ====")
		// printtree(tree)
		// log.Println("==== tree dump ====")
		// log.Println("-> inserting", r, i)
		if err := tree.Insert(r, i); err != nil && err != ErrExists {
			t.Fatalf("Failed to insert(%v, %v): %v", r, i, err)
		}
	}

	unique := []struct{key string; val int}{
		{"a", 2},
		{"b", 3},
		{"c", 9},
		{"d", 10},
		{"e", 0},
	}

	for _, u := range unique {
		v, found, err := tree.Find(u.key)
		if err != nil {
			t.Fatalf("Find(%v) unexpectedly failed", u.key)
		}
		if !found {
			t.Errorf("Find(%v) unexpectedly not found", u.key)
		}
		if v != u.val {
			t.Errorf("Find(%v) yielded %v, want %v", u.key, v, u.val)
		}
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

func TestScanFrom(t *testing.T) {
	store := InMemoryStore[rune, int]{}
	tree, err := New(&store, cmp, 8)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	alphabet := []rune("abcdefghijklmnopqrstuvwxyz")
	for i, r := range alphabet {
		if err := tree.Insert(r, i); err != nil {
			t.Fatalf("Failed to insert(%v, %v): %v", r, i, err)
		}
	}

	iter, err := tree.ScanFrom(rune('e'))
	if err != nil {
		t.Fatalf("ScanAll failed: %v", err)
	}

	for i := 4; i < len(alphabet); i++ {
		r := alphabet[i]
		if !iter.Next() {
			t.Fatalf("iterator stopped too early!")
		}
		k, v := iter.Current()
		if k != r {
			t.Errorf("Got key %c; want %c", k, r)
		}
		if v != i {
			t.Errorf("Got value %v; want %v", v, i)
		}
	}

	if iter.Next() {
		t.Fatalf("iterator could unexpectedly continue")
	}
}

func TestScanAllReverse(t *testing.T) {
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

	iter, err := tree.ScanAllReverse()
	if err != nil {
		t.Fatalf("ScanAll failed: %v", err)
	}

	for i := len(alphabet)-1; i >= 0; i-- {
		r := alphabet[i]
		if !iter.Next() {
			t.Fatalf("iterator stopped too early at %v (%c)", i, r)
		}
		k, v := iter.Current()
		if k != r {
			t.Errorf("Got key %c; want %c", k, r)
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

	alphabet := []rune("abcdefghijklmnopqrstuvwxyz")
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

func PathCmp(a, b string) int {
	da := strings.Count(a, "/")
	db := strings.Count(b, "/")

	if da > db {
		return 1
	}
	if da < db {
		return -1
	}
	return strings.Compare(a, b)
}

func TestBar(t *testing.T) {
	items := []string{
		"ui/v2/frontend/favicon.png",
		"ui/v2/ui.go",
		"storage/backends/database",
		".direnv/devenv.rc",
		"btree/btree_test.go",
		"CONTRIBUTING.md",
		".gitignore",

		// "/home/op/w/plakar/ui/v2/frontend/favicon.png",
		// "/home/op/w/plakar/ui/v2/ui.go",
		// "/home/op/w/plakar/storage/backends/database",
		// "/home/op/w/plakar/.direnv/devenv.rc",
		// "/home/op/w/plakar/btree/btree_test.go",
		// "/",
		// "/home/op/w/plakar/CONTRIBUTING.md",
		// "/home/op/w/plakar/.gitignore",
		// "/home/op/w/plakar/api/api_params.go",
		// "/home",
		// "/home/op/w/plakar/snapshot/importer/s3/s3.go",
		// "/home/op/w/plakar/snapshot/packer.go",


		// "c",
		// "b",
		// "a",

		// "/ui/v2/frontend/assets/index-CEMZEWSL.js",
		// "/cmd/plakar/subcommands/backup/stdio.go",
		// "/snapshot/store.go",
		// "/etc/.clean",
		// "/etc/.updated",
		// "/etc/NIXOS",
		// "/etc/.pwd.lock",
		// "/etc/NetworkManager/system-connections",
		// "/etc/ipsec.d",
		// "/etc/X11/xorg.conf.d/10-evdev.conf",
		// "/etc/modprobe.d/nixos.conf",
		// "/etc/ly/config.ini",
		// "/etc/fuse.conf",
		// "/etc/default",
	}

	order := 3
	store := InMemoryStore[string, int]{}
	tree, err := New(&store, PathCmp, order)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	for i, path := range items {
		if err := tree.Insert(path, i); err != nil {
			if err == ErrExists {
				continue
			}
			t.Fatalf("failed to insert %v: %v", path, err)
		}
	}

	for i, path := range items {
		val, found, err := tree.Find(path)
		if err != nil {
			t.Fatalf("failed to Find(%q): %v", path, err)
		}
		if !found {
			t.Errorf("key %v unexpectedly not found", path)
			continue
		}
		if val != i {
			t.Errorf("bad value for key %v: got %v want %v", path, val, i)
		}
	}
}

func TestFoo(t *testing.T) {
	files := []string{
		"/home/op",
		"/home/op/w/plakar/storage/backends",
		"/home/op/w/plakar/.github/workflows/codeql-analysis.yml",
		"/home/op/w/plakar/storage/backends/fs",
		"/home/op/w/plakar/storage/backends/http",
		"/home/op/w/plakar/storage/backends/null",
		"/home/op/w/plakar/storage/backends/plakard",
		"/home/op/w/plakar/ui/v2/frontend",
		"/home/op/w/plakar/ui/v2/ui.go",
		"/home/op/w/plakar/storage/backends/plakard/client.go",
		"/home/op/w/plakar/storage/backends/s3/s3.go",
		"/home/op/w/plakar/ui/v2/frontend/assets/IOS-C52CfiJx.png",
		"/home/op/w/plakar/storage/storage.go",
	}

	for run := 0; run < 100; run++ {
		items := make([]string, 0, len(files))
		copy(items, files[:])

		for i := range items {
			j := rand.Intn(i + 1)
			items[i], items[j] = items[j], items[i]
		}

		order := 3
		store := InMemoryStore[string, int]{}
		tree, err := New(&store, PathCmp, order)
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}

		for i, path := range items {
			if err := tree.Insert(path, i); err != nil {
				t.Fatalf("failed to insert %v: %v", path, err)
			}
		}

		for i, path := range items {
			val, found, err := tree.Find(path)
			if err != nil {
				t.Fatalf("failed to Find(%q): %v", path, err)
			}
			if !found {
				t.Errorf("key %v unexpectedly not found", path)
				continue
			}
			if val != i {
				t.Errorf("bad value for key %v: got %v want %v", path, val, i)
			}
		}
	}
}
