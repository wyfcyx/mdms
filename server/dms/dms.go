package main

import (
    "github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
    //"strconv"
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
)

var (
	UserMap map[string]uint32
	GroupMap map[uint16][]uint16
)

type Operation struct {
	Uid uint16 
	Gid uint16
	Command string
	Path string
	Args []string
	PairList []uint16
}

type LevelDB struct {
	//m sync.Mutex
	db *leveldb.DB
}

var levelDB *LevelDB


type Reply struct {
	R int
	Info string
	MDirAccess access.DirAccess
}

func DirentTransfer(path string) string {
    for i := len(path) - 2; i >= 0; i-- {
        if path[i] == '/' {
            path = path[:i] + "\\" + path[i + 1:]
            break
        }
    }
    return path
}

func DirentTransferBack(path string) string {
    return strings.Replace(path, "\\", "/", 1)
}

func Access(path string, t *LevelDB) (bool, string) {
	// Access path's father directory
	split := strings.Split(path, "/")
	split = split[:len(split) - 2]
	depth := len(split)
	curr := ""
	for i := 0; i < depth; i++ {
		curr = curr + split[i] + "/"
		if _, err := t.db.Get([]byte(DirentTransfer(curr)), nil); err != nil {
			return false, curr
		}
	}
	return true, ""
}

func Pass(path string, uid uint16, gid uint16, t *LevelDB) (bool, access.DirAccess) {
	log.Printf("pass uid,gid=%v,%v path=%v\n", uid, gid, path)
	// return if (uid, gid) can pass all the directories from / to <path>
	// search path as a key in KVS
	byteArray, err := t.db.Get([]byte(DirentTransfer(path)), nil)
	if err != nil {
		log.Printf("cannot found path %v in KVS\n", path)
		return false, access.DirAccess{}
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

func Stat(path string, t *LevelDB) access.DirAccess {
	fmt.Println("start Stat!")
	byteArray, err := t.db.Get([]byte(DirentTransfer(path)), nil)
	if err != nil {
		log.Fatalf("cannot found path %v in KVS\n", path)
	}
	return access.ByteArray2DirAccess(byteArray)
}

func (t *LevelDB) Query(operation Operation, reply *Reply) error {
	switch operation.Command {
	case "mkdir":
		// create directory, absolute path start with '/'
		// as a directory, its path must end with '/' as well
		// no access validation
		log.Printf("(uid,gid)=%v,%v mkdir %v", operation.Uid, operation.Gid, operation.Path);
		dirAccess := access.DirAccess {
			operation.Uid,
			operation.Gid,
			access.Ugo2Mode(7, 5, 5),
			// TODO: get mode config from args && update pairlist
			operation.PairList}
		if err := t.db.Put([]byte(DirentTransfer(operation.Path)), access.DirAccess2ByteArray(dirAccess), nil); err != nil {
			log.Panicln("leveldb error!")
		}
	case "ls":
		// now directory only
		// access father directory
		log.Printf("(uid,gid)=%v,%v ls %v", operation.Uid, operation.Gid, operation.Path)
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
		reply.R = 0
		reply.Info = t
	case "stat":
		// now directory only
		log.Printf("(uid,gid)=%v,%v stat %v", operation.Uid, operation.Gid, operation.Path)
		reply.R = 0
		reply.MDirAccess = Stat(operation.Path, t)
		log.Printf("stat ok!")
	case "pass":
		passed, dirAccess := Pass(operation.Path, operation.Uid, operation.Gid, t)
		if passed {
			log.Printf("passed!")
			reply.R = 0
			reply.MDirAccess = dirAccess
		} else {
			log.Printf("not passed!")
			reply.R = -1
		}
	}

	return nil
}

func initialize(db *leveldb.DB) {
	// create the root directory
	dirAccess := access.DirAccess{0, 0, access.Ugo2Mode(7, 5, 5), nil}
	if err := db.Put([]byte("/"), access.DirAccess2ByteArray(dirAccess), nil); err != nil {
		log.Fatalln("error when creating /")
	}
	// create home directoy holder for all users
	if err := db.Put([]byte(DirentTransfer("/home/")), access.DirAccess2ByteArray(dirAccess), nil); err != nil {
		log.Fatalln("error when creating /home/")
	}
	// create home directory for root
	if err := db.Put([]byte(DirentTransfer("/root/")), access.DirAccess2ByteArray(dirAccess), nil); err != nil {
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
		if err := db.Put([]byte(DirentTransfer(home)), access.DirAccess2ByteArray(dirAccess), nil); err != nil {
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
		return
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
		log.Fatal("listen error : ", e)
	}
	fmt.Println("ready serve!")
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
