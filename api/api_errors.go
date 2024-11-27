package api

import (
	"encoding/json"
	"errors"
	"net/http"
)

type ParameterError struct {
	Code    ParamErrorType `json:"code"`
	Message string         `json:"message"`
}

type ApiError struct {
	Code    string                    `json:"code"`
	Message string                    `json:"message"`
	Params  map[string]ParameterError `json:"params,omitempty"`
}

type ApiErrorRes struct {
	Error ApiError `json:"error"`
}

type ParamErrorType string

const (
	InvalidArgument ParamErrorType = "invalid_argument"
	BadNumber                      = "bad_number"
	MissingArgument                = "missing_argument"
)

var (
	ErrNegativeNumber = errors.New("Expected positive number")
	ErrMissingField   = errors.New("Missing field")
	ErrInvalidID      = errors.New("Invalid ID")
	ErrInvalidSortKey = errors.New("Invalid sort key")
)

func paramError(w http.ResponseWriter, field string, code ParamErrorType, e error) {
	h := w.Header()
	h.Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadRequest)

	err := ApiError{
		Code:    "invalid_params",
		Message: "The request is malformed",
		Params: map[string]ParameterError{
			field: {
				Code:    code,
				Message: e.Error(),
			},
		},
	}

	json.NewEncoder(w).Encode(ApiErrorRes{err})
}
