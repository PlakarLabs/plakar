package snapshot

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/PlakarKorp/plakar/search"
	"github.com/PlakarKorp/plakar/snapshot/vfs"
)

func (snap *Snapshot) matchFilename(fileEntry *vfs.FileEntry, f search.Filter) (bool, error) {
	value := f.Value
	if strings.HasPrefix(value, `"`) && strings.HasSuffix(value, `"`) {
		value = value[1 : len(value)-1]
	}

	var err error
	matched := false
	switch strings.ToLower(f.Operator) {
	case ":", "=":
		if fileEntry.Name() == value {
			matched = true
		}
	case "<>", "!=":
		if fileEntry.Name() != value {
			matched = true
		}
	case "<":
		if fileEntry.Name() < value {
			matched = true
		}
	case "<=":
		if fileEntry.Name() <= value {
			matched = true
		}
	case ">":
		if fileEntry.Name() > value {
			matched = true
		}
	case ">=":
		if fileEntry.Name() >= value {
			matched = true
		}
	case "~=":
		matched, err = regexp.MatchString(value, fileEntry.Name())
		if err != nil {
			return false, err
		}
	}

	return matched, nil
}

func (snap *Snapshot) matchContentType(fileEntry *vfs.FileEntry, f search.Filter) (bool, error) {
	value := f.Value
	if strings.HasPrefix(value, `"`) && strings.HasSuffix(value, `"`) {
		value = value[1 : len(value)-1]
	}

	var err error
	matched := false
	switch strings.ToLower(f.Operator) {
	case ":", "=":
		if fileEntry.ContentType() == value {
			matched = true
		}
	case "<>", "!=":
		if fileEntry.ContentType() != value {
			matched = true
		}
	case "<":
		if fileEntry.ContentType() < value {
			matched = true
		}
	case "<=":
		if fileEntry.ContentType() <= value {
			matched = true
		}
	case ">":
		if fileEntry.ContentType() > value {
			matched = true
		}
	case ">=":
		if fileEntry.ContentType() >= value {
			matched = true
		}
	case "~=":
		matched, err = regexp.MatchString(value, fileEntry.ContentType())
		if err != nil {
			return false, err
		}
	}

	return matched, nil
}

func (snap *Snapshot) matchSize(fileEntry *vfs.FileEntry, f search.Filter) (bool, error) {
	value := f.Value
	if strings.HasPrefix(value, `"`) && strings.HasSuffix(value, `"`) {
		value = value[1 : len(value)-1]
	}

	cmpValue, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return false, err
	}

	matched := false
	switch strings.ToLower(f.Operator) {
	case ":", "=":
		if fileEntry.Size() == cmpValue {
			matched = true
		}
	case "<>", "!=":
		if fileEntry.Size() != cmpValue {
			matched = true
		}
	case "<":
		if fileEntry.Size() < cmpValue {
			matched = true
		}
	case "<=":
		if fileEntry.Size() <= cmpValue {
			matched = true
		}
	case ">":
		if fileEntry.Size() > cmpValue {
			matched = true
		}
	case ">=":
		if fileEntry.Size() >= cmpValue {
			matched = true
		}
	}

	return matched, nil
}

func (snap *Snapshot) matchEntropy(fileEntry *vfs.FileEntry, f search.Filter) (bool, error) {
	value := f.Value
	if strings.HasPrefix(value, `"`) && strings.HasSuffix(value, `"`) {
		value = value[1 : len(value)-1]
	}

	cmpValue, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return false, err
	}

	matched := false
	switch strings.ToLower(f.Operator) {
	case ":", "=":
		if fileEntry.Entropy() == cmpValue {
			matched = true
		}
	case "<>", "!=":
		if fileEntry.Entropy() != cmpValue {
			matched = true
		}
	case "<":
		if fileEntry.Entropy() < cmpValue {
			matched = true
		}
	case "<=":
		if fileEntry.Entropy() <= cmpValue {
			matched = true
		}
	case ">":
		if fileEntry.Entropy() > cmpValue {
			matched = true
		}
	case ">=":
		if fileEntry.Entropy() >= cmpValue {
			matched = true
		}
	}

	return matched, nil
}

func (snap *Snapshot) searchMatch(fileEntry *vfs.FileEntry, q search.Query) (bool, error) {
	var err error
	leftMatch := false
	rightMatch := false

	switch strings.ToLower(q.Left.Field) {
	case "filename":
		leftMatch, err = snap.matchFilename(fileEntry, *q.Left)
		if err != nil {
			return false, err
		}
	case "contenttype":
		leftMatch, err = snap.matchContentType(fileEntry, *q.Left)
		if err != nil {
			return false, err
		}
	case "entropy":
		leftMatch, err = snap.matchEntropy(fileEntry, *q.Left)
		if err != nil {
			return false, err
		}
	case "size":
		leftMatch, err = snap.matchSize(fileEntry, *q.Left)
		if err != nil {
			return false, err
		}
	default:
		return false, fmt.Errorf("unsupported field: %s", q.Left.Field)
	}

	if q.Operator == nil {
		return leftMatch, nil
	}

	if *q.Operator == "AND" && !leftMatch {
		return false, nil
	}

	if q.Right != nil {
		rightMatch, err = snap.searchMatch(fileEntry, *q.Right)
		if err != nil {
			return false, err
		}
	}

	if *q.Operator == "AND" {
		return leftMatch && rightMatch, nil
	} else if *q.Operator == "OR" {
		return leftMatch || rightMatch, nil
	} else {
		return false, fmt.Errorf("unsupported operator: %s", *q.Operator)
	}
}

func (snap *Snapshot) search(fs *vfs.Filesystem, prefix string, q search.Query, c chan search.Result) error {
	for f := range fs.Pathnames() {
		if !strings.HasPrefix(f, prefix) {
			continue
		}

		fi, err := fs.Stat(f)
		if err != nil {
			return err
		}
		if fileEntry, isFile := fi.(*vfs.FileEntry); !isFile {
			continue
		} else {
			if match, err := snap.searchMatch(fileEntry, q); err != nil {
				return err
			} else if match {
				c <- search.FileEntry{
					Repository: snap.Repository().Location(),
					Snapshot:   snap.Header.SnapshotID,
					FileEntry:  *fileEntry,
				}
			}
		}
	}
	return nil
}

func (snap *Snapshot) Search(prefix string, query string) (chan search.Result, error) {
	c := make(chan search.Result)

	go func() {
		defer close(c)
		fs, err := snap.Filesystem()
		if err != nil {
			c <- search.Error{Message: err.Error()}
			return
		}

		if !strings.HasSuffix(prefix, "/") {
			prefix = prefix + "/"
		}

		q, err := search.Parse(query)
		if err != nil {
			c <- search.Error{Message: err.Error()}
			return
		}

		err = snap.search(fs, prefix, *q, c)
		if err != nil {
			c <- search.Error{Message: err.Error()}
			return
		}

	}()

	return c, nil
}
