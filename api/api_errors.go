package api

import (
	"errors"
	"net/http"
)

var (
	ErrNegativeNumber   = errors.New("expected positive number")
	ErrNumberOutOfRange = errors.New("number out of range")
	ErrMissingField     = errors.New("missing field")
	ErrInvalidID        = errors.New("invalid ID")
	ErrInvalidSortKey   = errors.New("invalid sort key")
	ErrInvalidFormat    = errors.New("invalid format")
)

type ParamErrorType string

const (
	InvalidArgument ParamErrorType = "invalid_argument"
	BadNumber       ParamErrorType = "bad_number"
	MissingArgument ParamErrorType = "missing_argument"
)

type ParameterError struct {
	Code    ParamErrorType `json:"code"`
	Message string         `json:"message"`
}

type ApiError struct {
	HttpCode int                       `json:"-"`
	ErrCode  string                    `json:"code"`
	Message  string                    `json:"message"`
	Params   map[string]ParameterError `json:"params,omitempty"`
}

func (a *ApiError) Error() string {
	return a.ErrCode + ": " + a.Message
}

func parameterError(field string, code ParamErrorType, message error) *ApiError {
	return &ApiError{
		HttpCode: http.StatusBadRequest,
		ErrCode:  "invalid_params",
		Message:  "Invalid parameter",
		Params: map[string]ParameterError{
			field: {
				Code:    code,
				Message: message.Error(),
			},
		},
	}
}

func authError(reason string) *ApiError {
	return &ApiError{
		HttpCode: http.StatusUnauthorized,
		ErrCode:  "bad_auth",
		Message:  reason,
	}
}
