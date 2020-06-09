package mrpc

import (
	"github.com/wyfcyx/mdms/access"
)

type DOperation struct {
	Uid uint16 
	Gid uint16
	Command string
	Path string
	Args []string
	PairList []uint16
}

type DReply struct {
	R int
	Info string
	MDirAccess access.DirAccess
}
