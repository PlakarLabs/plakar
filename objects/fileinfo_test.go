package objects

import (
	"testing"
	"time"
)

func TestParseFileInfoSortKeys(t *testing.T) {
	for _, test := range []struct {
		Sort  string
		Error string
		Keys  []string
	}{
		{
			Sort:  "",
			Error: "",
			Keys:  nil,
		},
		{
			Sort:  "Name,Name",
			Error: "duplicate sort key: Name",
			Keys:  nil,
		},
		{
			Sort:  "Name,Invalid",
			Error: "invalid sort key: Invalid",
			Keys:  nil,
		},
		{
			Sort:  "Mode,-Gid,Name",
			Error: "",
			Keys:  []string{"Mode", "-Gid", "Name"},
		},
	} {
		t.Run(test.Sort, func(t *testing.T) {
			keys, err := ParseFileInfoSortKeys(test.Sort)
			if err != nil {
				if err.Error() != test.Error {
					t.Fatalf("Expected %s but got %v", test.Error, err)
				}
			} else {
				if test.Error != "" {
					t.Fatalf("Expected %s but got nil", test.Error)
				}
				if len(keys) != len(test.Keys) {
					t.Fatalf("Expected %v but got %v", test.Keys, keys)
				}
				for i := range keys {
					if keys[i] != test.Keys[i] {
						t.Fatalf("Expected %v but got %v", test.Keys, keys)
					}
				}
			}
		})
	}
}

func TestSortFileInfos(t *testing.T) {
	infos := []FileInfo{
		{Lname: "file1", Lsize: 300, Lmode: 0644, LmodTime: time.Now(), Ldev: 0, Lino: 0, Luid: 0, Lgid: 0, Lnlink: 1},
		{Lname: "file2", Lsize: 400, Lmode: 0644, LmodTime: time.Now(), Ldev: 0, Lino: 0, Luid: 0, Lgid: 0, Lnlink: 1},
		{Lname: "file3", Lsize: 100, Lmode: 0644, LmodTime: time.Now(), Ldev: 0, Lino: 0, Luid: 0, Lgid: 0, Lnlink: 1},
		{Lname: "file4", Lsize: 100, Lmode: 0644, LmodTime: time.Now(), Ldev: 0, Lino: 0, Luid: 0, Lgid: 0, Lnlink: 1},
	}

	for _, test := range []struct {
		Sort     string
		Expected []FileInfo
	}{
		{
			Sort:     "Name",
			Expected: []FileInfo{infos[0], infos[1], infos[2], infos[3]},
		},
		{
			Sort:     "Size",
			Expected: []FileInfo{infos[2], infos[3], infos[0], infos[1]},
		},
		{
			Sort:     "-Size",
			Expected: []FileInfo{infos[1], infos[0], infos[2], infos[3]},
		},
		{
			Sort:     "Size,-Name",
			Expected: []FileInfo{infos[3], infos[2], infos[0], infos[1]},
		},
	} {
		t.Run(test.Sort, func(t *testing.T) {
			keys, err := ParseFileInfoSortKeys(test.Sort)
			if err != nil {
				t.Fatalf("Expected nil but got %v", keys)
			}
			err = SortFileInfos(infos, keys)
			if err != nil {
				t.Fatalf("Expected nil but got %v", err)
			}
			for i := range test.Expected {
				if infos[i] != test.Expected[i] {
					t.Fatalf("Expected %v but got %v", test.Expected[i], infos[i])
				}
			}
		})
	}
}
