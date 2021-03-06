package access

import (
	"fmt"
	"log"
	"encoding/gob"
	"bytes"

)

type DirAccess struct {
	Uid uint16
	Gid uint16
	Mode uint16
	PairList []uint16
}

func (dirAccess DirAccess) GetString() string {
	//u, g, o := Mode2Ugo(dirAccess.Mode)
	return fmt.Sprintf("uid=%v gid=%v %v", dirAccess.Uid, dirAccess.Gid, Mode2String(dirAccess.Mode))
}

func DirAccess2ByteArray(dirAccess DirAccess) []byte {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	if err := enc.Encode(dirAccess); err != nil {
		log.Fatalln("error when encoding dirAccess: ", err)
	}
	return buf.Bytes()
}

func ByteArray2DirAccess(byteArray []byte) DirAccess {
	dec := gob.NewDecoder(bytes.NewBuffer(byteArray))
	var dirAccess DirAccess
	if err := dec.Decode(&dirAccess); err != nil {
		log.Fatalln("error when decoding dirAccess: ", err)
	}
	return dirAccess	
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
