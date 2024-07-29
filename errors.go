package ctrl

import "fmt"

type QueryError struct {
	statuscode int
	msg        string
}

func newQueryError(statusCode int, msg string) *QueryError {
	return &QueryError{statusCode, msg}
}

func (a *QueryError) Error() string {
	s := fmt.Sprintf("API Query returned error, status code: %d", a.statuscode)
	if a.msg != "" {
		return fmt.Sprintf("%s, msg: %s", s, a.msg)
	}

	return s
}
