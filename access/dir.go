package access

import (
	"fmt"
)

type DirAccess struct {
	Uid uint16
	Gid uint16
	Mode uint16
	PairList []uint16
}

func (dirAccess DirAccess) GetString() string {
	u, g, o := Mode2Ugo(dirAccess.Mode)
	return fmt.Sprintf("uid=%v gid=%v ugo=%v%v%v", dirAccess.Uid, dirAccess.Gid, u, g, o)
}

func Ugo2Mode (u uint16, g uint16, o uint16) uint16 {
	return u * 64 + g * 8 + o
}

func Mode2Ugo(mode uint16) (uint16, uint16, uint16) {
	return (mode >> 6) & 7, (mode >> 3) & 7, mode & 7
}

func (dirAccess DirAccess) DirAccessCheck(uid uint16, gid uint16, flag uint16) bool {
	// flag & 1 -> need X
	// flag & 2 -> need W
	// flag & 4 -> need R
	u, g, o := Mode2Ugo(dirAccess.Mode)	
	if uid == dirAccess.Uid {
		return u & flag == flag
	} else if gid == dirAccess.Gid {
		// TODO: fetch full Gidlist(uid) from group file
		return g & flag == flag
	} else {
		return o & flag == flag
	}
}
