package mrpc

import (
	//"github.com/wyfcyx/mdms/access"
)

type FOperation struct {
	Uid uint16
	Gid uint16
	Command string
	Path string
	Args []string
}

type FReply struct {
	R int
	Info string
}
