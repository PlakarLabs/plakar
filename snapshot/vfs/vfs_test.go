package vfs

import (
	"log"
	"slices"
	"testing"
)

func ppslice(s []string) {
	for _, x := range s {
		log.Println("->", x)
	}
}

func TestPathCmp(t *testing.T) {
	suite := []struct{
		input []string
		expect []string
	}{
		{
			input: []string{"/"},
			expect: []string{"/"},
		},
		{
			input: []string{"c", "a", "b", "z"},
			expect: []string{"a", "b", "c", "z"},
		},
		{
			input: []string{"c", "a", "b", "z", "z/a", "z/z", "z/a/b/c", "z/z/z", "z/a/z"},
			expect: []string{"a", "b", "c", "z", "z/a", "z/z", "z/a/z", "z/z/z", "z/a/b/c"},
		},
		{
			input: []string{"/etc/", "/"},
			expect: []string{"/", "/etc/"},
		},
		{
			input: []string{"/etc/zzz", "/etc/foo/bar", "/etc/foo"},
			expect: []string{"/etc/foo", "/etc/zzz", "/etc/foo/bar"},
		},
		{
			input: []string{"/etc/zzz", "/etc/foo/bar", "/etc/foo", "/etc/aaa"},
			expect: []string{"/etc/aaa", "/etc/foo", "/etc/zzz", "/etc/foo/bar"},
		},
		{
			input: []string{"/home/op", "/etc/zzz", "/etc/foo/bar", "/etc/foo", "/home/op/.kshrc"},
			expect: []string{"/etc/foo", "/etc/zzz", "/home/op", "/etc/foo/bar", "/home/op/.kshrc"},
		},
		{
			input: []string{
				"/",
				"/home",
				"/home/op",
				"/home/op/w",
				"/home/op/w/plakar",
				"/home/op/w/plakar/btree",
				"/home/op/w/plakar/btree/btree.go",
				"/home/op/w/plakar/storage",
				"/home/op/w/plakar/storage/backends",
				"/home/op/w/plakar/storage/backends/database",
				"/home/op/w/plakar/storage/backends/database/database.go",
				"/home/op/w/plakar/storage/backends/null",
				"/home/op/w/plakar/storage/backends/null/null.go",
				"/home/op/w/plakar/storage/backends/s3",
				"/home/op/w/plakar/storage/backends/s3/s3.go",
				"/home/op/w/plakar/snapshot",
				"/home/op/w/plakar/snapshot/backup.go",
				"/home/op/w/plakar/snapshot/exporter",
				"/home/op/w/plakar/snapshot/exporter/exporter.go",
				"/home/op/w/plakar/snapshot/exporter/fs",
				"/home/op/w/plakar/snapshot/exporter/fs/fs.go",
				"/home/op/w/plakar/snapshot/vfs",
				"/home/op/w/plakar/snapshot/vfs/vfs.go",
				"/home/op/w/plakar/snapshot/vfs/entry.go",
			},
			expect: []string{
				"/",
				"/home",
				"/home/op",
				"/home/op/w",
				"/home/op/w/plakar",
				"/home/op/w/plakar/btree",
				"/home/op/w/plakar/snapshot",
				"/home/op/w/plakar/storage",
				"/home/op/w/plakar/btree/btree.go",
				"/home/op/w/plakar/snapshot/backup.go",
				"/home/op/w/plakar/snapshot/exporter",
				"/home/op/w/plakar/snapshot/vfs",
				"/home/op/w/plakar/storage/backends",
				"/home/op/w/plakar/snapshot/exporter/exporter.go",
				"/home/op/w/plakar/snapshot/exporter/fs",
				"/home/op/w/plakar/snapshot/vfs/entry.go",
				"/home/op/w/plakar/snapshot/vfs/vfs.go",
				"/home/op/w/plakar/storage/backends/database",
				"/home/op/w/plakar/storage/backends/null",
				"/home/op/w/plakar/storage/backends/s3",
				"/home/op/w/plakar/snapshot/exporter/fs/fs.go",
				"/home/op/w/plakar/storage/backends/database/database.go",
				"/home/op/w/plakar/storage/backends/null/null.go",
				"/home/op/w/plakar/storage/backends/s3/s3.go",
			},
		},
	}

	for _, test := range suite {
		sorted := make([]string, len(test.input))
		copy(sorted, test.input)

		slices.SortFunc(sorted, PathCmp)
		if slices.Compare(test.expect, sorted) != 0 {
			t.Error("expected:")
			ppslice(test.expect)
			t.Error("got:")
			ppslice(sorted)
		}

		for _, path := range test.input {
			if _, found := slices.BinarySearchFunc(sorted, path, PathCmp); !found {
				t.Error("item not found by binary search:", path)
			}
		}
	}
}
