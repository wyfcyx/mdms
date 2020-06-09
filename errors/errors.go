package errors

import (
	"log"
)

const (
	OK int = 0
	ACCESS_DENIED int = -1
	NO_SUCH_FILEDIR int = -2
	FILEDIR_EXISTED int = -3
	MODE_INVALID int = -4
)

func ErrorString(rc int) string {
	switch rc {
	case OK:
		return "OK"
	case ACCESS_DENIED:
		return "ACCESS_DENIED"
	case NO_SUCH_FILEDIR:
		return "NO_SUCH_FILEDIR"
	case FILEDIR_EXISTED:
		return "FILEDIR_EXISTED"
	case MODE_INVALID:
		return "MODE_INVALID"
	default:
		log.Fatalln("unrecognized error")
	}
	return ""
}

