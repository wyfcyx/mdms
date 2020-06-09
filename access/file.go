package access

import (
	"fmt"
	"bytes"
	"encoding/gob"
	"log"
)

type FileAccess struct {
	Uid uint16
	Gid uint16
	Mode uint16
}

func (fileAccess FileAccess) GetString() string {
	//u, g, o := Mode2Ugo(fileAccess.Mode)
	return fmt.Sprintf("uid=%v gid=%v %v", fileAccess.Uid, fileAccess.Gid, Mode2String(fileAccess.Mode))
}

func FileAccess2ByteArray(fileAccess FileAccess) []byte {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	if err := enc.Encode(fileAccess); err != nil {
		log.Fatalln("error when encoding fileAccess: ", err)
	}
	return buf.Bytes()
}

func ByteArray2FileAccess(byteArray []byte) FileAccess {
	dec := gob.NewDecoder(bytes.NewBuffer(byteArray))
	var fileAccess FileAccess
	if err := dec.Decode(&fileAccess); err != nil {
		log.Fatalln("error when decoding fileAccess: ", err)
	}
	return fileAccess
}

func (fileAccess FileAccess) FileAccessCheck(uid uint16, gid uint16, flag uint16) bool {
	u, g, o := Mode2Ugo(fileAccess.Mode)
	if uid == fileAccess.Uid {
		return u & flag == flag
	} else if gid == fileAccess.Gid {
		// TODO: fetch full Gidlist(uid) from group file
		return g & flag == flag
	} else {
		return o & flag == flag
	}
}
