package core

type Error string

// errors
const (
	UnknownMessage Error = "unknown message"
	NotImplemented Error = "functionality not (yet) implemented"
	Timeout        Error = "timeout reached"
	NoError        Error = ""
	AbsPathError   Error = "failed to determine the absolute path"
)

func (e Error) Error() string {
	return string(e)
}
