package api

import (
	"encoding/hex"
	"math"
	"net/http"
	"strconv"

	"github.com/PlakarKorp/plakar/snapshot/header"
	"github.com/gorilla/mux"
)

func PathParamToID(r *http.Request, param string) (id [32]byte, err error) {
	vars := mux.Vars(r)
	idstr := vars[param]

	if idstr == "" {
		return id, parameterError(param, MissingArgument, ErrMissingField)
	}

	b, err := hex.DecodeString(idstr)
	if err != nil {
		return id, parameterError(param, InvalidArgument, err)
	}

	if len(b) != 32 {
		return id, parameterError(param, InvalidArgument, ErrInvalidID)
	}

	copy(id[:], b)
	return id, nil
}

func QueryParamToUint32(r *http.Request, param string) (uint32, bool, error) {
	str := r.URL.Query().Get(param)
	if str == "" {
		return 0, false, nil
	}

	n, err := strconv.ParseInt(str, 10, 32)
	if err != nil {
		return 0, true, err
	}

	if n < 0 {
		return 0, true, parameterError(param, BadNumber, ErrNegativeNumber)
	}

	if n > math.MaxUint32 {
		return 0, true, parameterError(param, BadNumber, ErrNumberOutOfRange)
	}

	return uint32(n), true, nil
}

func QueryParamToSortKeys(r *http.Request, param, def string) ([]string, error) {
	str := r.URL.Query().Get(param)
	if str == "" {
		str = def
	}

	sortKeys, err := header.ParseSortKeys(str)
	if err != nil {
		return []string{}, parameterError(param, InvalidArgument, err)
	}

	return sortKeys, nil
}
