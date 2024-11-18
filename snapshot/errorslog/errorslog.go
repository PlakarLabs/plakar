package errorslog

import "github.com/vmihailenco/msgpack/v5"

type ErrorLogEntry struct {
	Pathname string `msgpack:"pathname", json:"pathname"`
	Error    string `msgpack:"error", json:"error"`
}

type ErrorsLog struct {
	Errors []ErrorLogEntry `msgpack:"errors", json:"errors"`
}

func NewErrorsLog() *ErrorsLog {
	return &ErrorsLog{}
}

func (e *ErrorsLog) Append(pathname string, errmsg string) {
	e.Errors = append(e.Errors, ErrorLogEntry{Pathname: pathname, Error: errmsg})
}

func (e *ErrorsLog) GetErrors() []ErrorLogEntry {
	return e.Errors
}

func (e *ErrorsLog) Serialize() ([]byte, error) {
	return msgpack.Marshal(e)
}

func FromBytes(data []byte) (*ErrorsLog, error) {
	e := NewErrorsLog()
	err := msgpack.Unmarshal(data, e)
	if err != nil {
		return nil, err
	}
	return e, err
}
