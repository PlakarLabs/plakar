package snapshot

import (
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/PlakarKorp/plakar/search"
	"github.com/PlakarKorp/plakar/snapshot/vfs"
)

func (snap *Snapshot) searchFilename(fs *vfs.Filesystem, q search.Query) ([]search.Result, error) {
	//fmt.Println(q)
	value := q.Left.Value
	if strings.HasPrefix(value, `"`) && strings.HasSuffix(value, `"`) {
		value = value[1 : len(value)-1]
	}

	ret := make([]search.Result, 0)
	for f := range fs.Files() {
		var include bool
		switch strings.ToLower(q.Left.Operator) {
		case ":", "=":
			if filepath.Base(f) == value {
				include = true
			}
		case "<>", "!=":
			if filepath.Base(f) != value {
				include = true
			}
		case "<":
			if filepath.Base(f) < value {
				include = true
			}
		case "<=":
			if filepath.Base(f) <= value {
				include = true
			}
		case ">":
			if filepath.Base(f) > value {
				include = true
			}
		case ">=":
			if filepath.Base(f) >= value {
				include = true
			}
		case "~":
		case "~=":
			matched, err := regexp.MatchString(value, filepath.Base(f))
			if err == nil && matched {
				include = true
			}
		}

		if include {
			ret = append(ret, search.Filename{
				Repository: snap.Repository().Location(),
				Snapshot:   snap.Header.SnapshotID,
				Path:       f,
			})
		}
	}
	return ret, nil
}

func (snap *Snapshot) searchSize(fs *vfs.Filesystem, q search.Query) ([]search.Result, error) {
	value := q.Left.Value
	if strings.HasPrefix(value, `"`) && strings.HasSuffix(value, `"`) {
		value = value[1 : len(value)-1]
	}

	cmpValue, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return nil, err
	}

	ret := make([]search.Result, 0)
	for f := range fs.Pathnames() {
		fi, err := fs.Stat(f)
		if err != nil {
			return nil, err
		}

		var include bool
		switch strings.ToLower(q.Left.Operator) {
		case ":", "=":
			if fi.Size() == cmpValue {
				include = true
			}
		case "<>", "!=":
			if fi.Size() != cmpValue {
				include = true
			}
		case "<":
			if fi.Size() < cmpValue {
				include = true
			}
		case "<=":
			if fi.Size() <= cmpValue {
				include = true
			}
		case ">":
			if fi.Size() > cmpValue {
				include = true
			}
		case ">=":
			if fi.Size() >= cmpValue {
				include = true
			}
		}
		if include {
			ret = append(ret, search.Filename{
				Repository: snap.Repository().Location(),
				Snapshot:   snap.Header.SnapshotID,
				Path:       f,
			})
		}
	}
	return ret, nil
}

func (snap *Snapshot) search(fs *vfs.Filesystem, q search.Query) ([]search.Result, error) {
	if q.Left == nil {
		return nil, nil
	}

	var err error
	var leftResults []search.Result
	var rightResults []search.Result

	switch q.Left.Field {
	case "filename":
		leftResults, err = snap.searchFilename(fs, q)
		if err != nil {
			return nil, err
		}
	case "size":
		leftResults, err = snap.searchSize(fs, q)
		if err != nil {
			return nil, err
		}
	default:
		return []search.Result{}, nil
	}

	if q.Operator == nil {
		return leftResults, nil
	}

	if q.Right != nil {
		rightResults, err = snap.search(fs, *q.Right)
		if err != nil {
			return nil, err
		}
	}

	if *q.Operator == "AND" {
		intersection := []search.Result{}
		resultMap := make(map[string]struct{})

		for _, result := range leftResults {
			key := result.Pathname()
			resultMap[key] = struct{}{}
		}

		for _, result := range rightResults {
			key := result.Pathname()
			if _, exists := resultMap[key]; exists {
				intersection = append(intersection, result)
			}
		}
		return intersection, nil
	} else if *q.Operator == "OR" {
		return append(leftResults, rightResults...), nil
	} else {
		return nil, fmt.Errorf("unsupported operator: %s", *q.Operator)
	}

	return rightResults, nil
}

func (snap *Snapshot) Search(query string) ([]search.Result, error) {
	fs, err := snap.Filesystem()
	if err != nil {
		return nil, err
	}

	q, err := search.Parse(query)
	if err != nil {
		return nil, err
	}

	resultSet, err := snap.search(fs, *q)
	if err != nil {
		return nil, err
	}

	sort.Slice(resultSet, func(i, j int) bool {
		return resultSet[i].Pathname() < resultSet[j].Pathname()
	})
	return resultSet, nil
}
