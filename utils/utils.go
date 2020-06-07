package utils

import (
	"os"
	"os/user"
	"log"
)

func GetFatherDirectory(path string) string {
	for i := len(path) - 2; i >= 0; i-- {
		if path[i] == '/' {
			return path[:i + 1]
		}
	}
	return path
}

func Home() string {
	user, err := user.Current()
	if err == nil {
		return user.HomeDir
	} else {
		log.Fatalln("error when getting home directory")
	}
	return ""
}

func Exists(path string) bool {
	_, err := os.Stat(path)
	if err != nil {
		if os.IsExist(err) {
			return true
		}
		return false
	}
	return true
}

func Uint16InArray(u uint16, a []uint16) bool {
	for _, v := range(a) {
		if v == u {
			return true
		}
	}
	return false
}
