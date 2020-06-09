package main

import (
    "github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
    "strconv"
    "fmt"
    "net"
    //"net/http"
    "net/rpc"
    "log"
	//"sync"
	"strings"
	//"bytes"
	//"encoding/gob"
	"os"

	"github.com/wyfcyx/mdms/access"
	"github.com/wyfcyx/mdms/utils"
	"github.com/wyfcyx/mdms/errors"
	"github.com/wyfcyx/mdms/mrpc"
)

var (
	UserMap map[string]uint32
	GroupMap map[uint16][]uint16
)

const (
	Debug bool = true
)

type LevelDB struct {
	//m sync.Mutex
	db *leveldb.DB
}

var levelDB *LevelDB

func Access(path string, t *LevelDB) (bool, string) {
	// Access path's father directory
	split := strings.Split(path, "/")
	split = split[:len(split) - 2]
	depth := len(split)
	curr := ""
	for i := 0; i < depth; i++ {
		curr = curr + split[i] + "/"
		if _, err := t.db.Get([]byte(utils.DirentTransfer(curr)), nil); err != nil {
			return false, curr
		}
	}
	return true, ""
}

func Pass(path string, uid uint16, gid uint16, t *LevelDB) (bool, access.DirAccess) {
	if Debug {
		log.Printf("pass uid,gid=%v,%v path=%v\n", uid, gid, path)
	}
	// return if (uid, gid) can pass all the directories from / to <path>
	// search path as a key in KVS
	byteArray, err := t.db.Get([]byte(utils.DirentTransfer(path)), nil)
	if err != nil {
		log.Fatalf("cannot found path %v in Pass\n", path)
	}
	
	// change byteArray into DirAccess
	dirAccess := access.ByteArray2DirAccess(byteArray)
	// check if (uid, gid) pair is valid
	// if File uid != given uid, then File gid must be included in Gidlist(given uid)
	if dirAccess.PairList != nil && len(dirAccess.PairList) > 0 {
		depth := len(dirAccess.PairList) / 2
		for i := 0; i < depth; i++ {
			fuid, fgid := dirAccess.PairList[i << 1], dirAccess.PairList[(i << 1) | 1]
			gidList, ok := GroupMap[uid]
			if !ok {
				log.Fatalf("uid=%v not found in GroupMap\n", uid)
			}
			if fuid != uid && !utils.Uint16InArray(fgid, gidList) {
				return false, access.DirAccess{} 
			}
		}
	}
	return true, dirAccess
}

func Stat(path string, t *LevelDB) (int, access.DirAccess) {
	byteArray, err := t.db.Get([]byte(utils.DirentTransfer(path)), nil)
	if err != nil {
		return errors.NO_SUCH_FILEDIR, access.DirAccess{}
	}
	return errors.OK, access.ByteArray2DirAccess(byteArray)
}

func DirExisted(path string, t *LevelDB) bool {
	has, err := t.db.Has([]byte(utils.DirentTransfer(path)), nil)
	if err != nil {
		log.Fatalln("error when querying dir existed")
	}
	return has
}

func (t *LevelDB) Query(operation mrpc.DOperation, reply *mrpc.DReply) error {
	switch operation.Command {
	case "mkdir":
		// create directory, absolute path start with '/'
		// as a directory, its path must end with '/' as well
		// no access validation
		if Debug {
			log.Printf("(uid,gid)=%v,%v mkdir %v", operation.Uid, operation.Gid, operation.Path);
		}
		if DirExisted(operation.Path, t) {
			reply.R = errors.FILEDIR_EXISTED
			break
		}
		var mode uint16
		if len(operation.Args) == 0 {
			mode = access.Ugo2Mode(7, 5, 5)
		} else {
			modeInt, err := strconv.Atoi(operation.Args[0])
			if err != nil {
				log.Fatalln("error when parsing mkdir: ", err)
			}
			mode = uint16(modeInt)
		}

		u, g, o := access.Mode2Ugo(mode)
		// assert o is a subset of g && g is a subset of u
		if !((g & o == o) && (u & g == g)) {
			reply.R = errors.MODE_INVALID
			break
		}

		dirAccess := access.DirAccess {
			operation.Uid,
			operation.Gid,
			mode,
			operation.PairList}
		if err := t.db.Put([]byte(utils.DirentTransfer(operation.Path)), access.DirAccess2ByteArray(dirAccess), nil); err != nil {
			log.Panicln("leveldb error!")
		}
		reply.R = errors.OK
	case "rmdir":
		if Debug {
			log.Printf("(uid,gid)=%v,%v rmdir %v", operation.Uid, operation.Gid, operation.Path)
		}
		if !DirExisted(operation.Path, t) {
			reply.R = errors.NO_SUCH_FILEDIR
			break
		}
		// remove all direct/indirect sub-directories like "ls"
		prefixL := operation.Path
		prefixR := operation.Path[:len(operation.Path) - 1] + "]"
		iter := t.db.NewIterator(
			&util.Range {
				Start: []byte(prefixL),
				Limit: []byte(prefixR),
			},
			nil,
		)
		for iter.Next() {
			if err := t.db.Delete(iter.Key(), nil); err != nil {
				log.Fatalln("error in leveldb: ", err)
			}
		}
		// remove itself
		if err := t.db.Delete([]byte(utils.DirentTransfer(operation.Path)), nil); err != nil {
			log.Fatalln("error in leveldb: ", err)
		}
		reply.R = errors.OK
	case "ls":
		// now directory only
		// access father directory
		if Debug {
			log.Printf("(uid,gid)=%v,%v ls %v", operation.Uid, operation.Gid, operation.Path)
		}
		if !DirExisted(operation.Path, t) {
			reply.R = errors.NO_SUCH_FILEDIR
			break
		}
		// get all directories which start at given path
		prefixL := operation.Path[:len(operation.Path) - 1] + "\\"
		prefixR := strings.Replace(prefixL, "\\", "]", 1)
		iter := t.db.NewIterator(
			&util.Range{
				Start: []byte(prefixL),
				Limit: []byte(prefixR),
			}, nil)
		t := ""
		for iter.Next() {
			t = t + "\n" + strings.Replace(string(iter.Key()), prefixL, "", 1)
		}
		iter.Release()
		reply.R = errors.OK 
		reply.Info = t
		// TODO: check whether the dir is existed
	case "stat":
		// now directory only
		if Debug {
			log.Printf("(uid,gid)=%v,%v stat %v", operation.Uid, operation.Gid, operation.Path)
		}
		reply.R, reply.MDirAccess = Stat(operation.Path, t)
	case "pass":
		if !DirExisted(operation.Path, t) {
			reply.R = errors.NO_SUCH_FILEDIR
			break
		}
		passed, dirAccess := Pass(operation.Path, operation.Uid, operation.Gid, t)
		if passed {
			reply.R = errors.OK 
			reply.MDirAccess = dirAccess
		} else {
			reply.R = errors.ACCESS_DENIED
		}
	case "access":
		if Debug {
			log.Printf("(uid,gid)=%v,%v access %v", operation.Uid, operation.Gid, operation.Path)
		}
		if !DirExisted(operation.Path, t) {
			reply.R = errors.NO_SUCH_FILEDIR
			break
		}
		byteArray, err := t.db.Get([]byte(utils.DirentTransfer(operation.Path)), nil)
		if err != nil {
			log.Fatalln("error in LevelDB: ", err)
		}
		dirAccess := access.ByteArray2DirAccess(byteArray)
		modeInt, err := strconv.Atoi(operation.Args[0])
		if err != nil {
			log.Fatalln("error when parsing access arguments")
		}
		mode := uint16(modeInt)
		if mode > 0 && !dirAccess.DirAccessCheck(operation.Uid, operation.Gid, mode) {
			reply.R = errors.ACCESS_DENIED
		} else {
			reply.R = errors.OK
		}
	}

	return nil
}

func initialize(db *leveldb.DB) {
	// create the root directory
	dirAccess := access.DirAccess{0, 0, access.Ugo2Mode(7, 5, 5), nil}
	if err := db.Put([]byte(utils.DirentTransfer("/")), access.DirAccess2ByteArray(dirAccess), nil); err != nil {
		log.Fatalln("error when creating /")
	}
	// create home directoy holder for all users
	if err := db.Put([]byte(utils.DirentTransfer("/home/")), access.DirAccess2ByteArray(dirAccess), nil); err != nil {
		log.Fatalln("error when creating /home/")
	}
	// create home directory for root
	if err := db.Put([]byte(utils.DirentTransfer("/root/")), access.DirAccess2ByteArray(dirAccess), nil); err != nil {
		log.Fatalln("error when creating /root/")
	}
	// create home directory for users other than root
	for username, uidGid := range(UserMap) {
		uid, gid := uint16(uidGid >> 16), uint16(uidGid & 65535)
		if uid == 0 {
			continue
		}
		dirAccess = access.DirAccess{uid, gid, access.Ugo2Mode(7, 5, 5), nil}
		log.Printf("user=%v uid,gid=%v,%v\n", username, uid, gid)
		home := "/home/" + username + "/"
		if err := db.Put([]byte(utils.DirentTransfer(home)), access.DirAccess2ByteArray(dirAccess), nil); err != nil {
			log.Fatalln("error when creating /home/" + username + "/")
		}
	}
}

func main() {
	mdmsHome := utils.Home() + "/.mdms/"
	// read passwd file get user list
	UserMap = access.LoadUserConfig(mdmsHome + "passwd")
	// read group file get group info
	GroupMap = access.LoadGroupConfig(mdmsHome + "group", UserMap)

	// check if previous db store exist & delete
	dbPath := mdmsHome + "dmsdb"
	if utils.Exists(dbPath) {
		if err := os.RemoveAll(dbPath); err != nil {
			log.Fatalln("error when remove previous db: ", err)
		}
	}	

	// create & open database
	db, err := leveldb.OpenFile(dbPath, nil)
	if err != nil {
		fmt.Println(err)
		return
	}

	initialize(db)

	levelDB = &LevelDB{db: db}
	//rpc.Register(levelDB)
	//rpc.HandleHTTP()
	server := rpc.NewServer()
	server.Register(levelDB)

	l, e := net.Listen("tcp", "10.1.0.20:1234")
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	fmt.Println("ready serving!")
	//http.Serve(l, nil)
	conCount := 0
	for {
		conn, err := l.Accept()
		if err != nil {
			log.Fatal("Accept Error: ", err)
			continue
		}
		conCount++
		fmt.Printf("now we have %v clients connected\n", conCount)
		go server.ServeConn(conn)
	}

}
