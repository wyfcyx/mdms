package utils

import (
	"os/user"
	"log"
)

func Home() string {
	user, err := user.Current()
	if err == nil {
		return user.HomeDir
	} else {
		log.Fatalln("error when getting home directory")
	}
	return ""
}
