package main

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"fmt"
	"net/rpc"
	"log"
	"os"
	"net"
	"strings"
	"strconv"

	"github.com/wyfcyx/mdms/access"
	"github.com/wyfcyx/mdms/utils"
	"github.com/wyfcyx/mdms/errors"
)

var (
	UserMap map[string]uint32
	GroupMap map[uint16][]uint16
)

const (
	Debug bool = false
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

func FileExisted(path string, t *LevelDB) bool {
	has, err := t.db.Has([]byte(utils.DirentTransfer(path)), nil)
	if err != nil {
		log.Fatalln("error when querying file existed")
	}
	return has
}

func (t *LevelDB) Query(operation Operation, reply *Reply) error {
	switch operation.Command {
	case "create":
		if Debug {
			log.Printf("(uid,gid)=%v,%v create %v", operation.Uid, operation.Gid, operation.Path)
		}
		fileAccess := access.FileAccess {
			operation.Uid,
			operation.Gid,
			access.Ugo2Mode(6, 4, 4)}
		pathByteArray := []byte(utils.DirentTransfer(operation.Path))
		if FileExisted(operation.Path, t) {
			reply.R = errors.FILEDIR_EXISTED
			break
		}
		if err := t.db.Put(pathByteArray, access.FileAccess2ByteArray(fileAccess), nil); err != nil {
			log.Fatalln("leveldb error!")
		}
		reply.R = errors.OK
	case "delete":
		if Debug {
			log.Printf("(uid,gid)=%v,%v delete %v", operation.Uid, operation.Gid, operation.Path)
		}
		if !FileExisted(operation.Path, t) {
			reply.R = errors.NO_SUCH_FILEDIR
			break
		}	
		if err := t.db.Delete([]byte(utils.DirentTransfer(operation.Path)), nil); err != nil {
			log.Fatalln("leveldb error!")
		}
		reply.R = errors.OK
	case "rmdir":
		if Debug {
			log.Printf("(uid,gid)=%v,%v rmdir %v", operation.Uid, operation.Gid, operation.Path)
		}
		// remove all direct/indirect sub-files
		prefixL := operation.Path
		prefixR := operation.Path[:len(operation.Path) - 1] + "]"
		iter := t.db.NewIterator(
			&util.Range {
				Start: []byte(prefixL),
				Limit: []byte(prefixR),
			}, nil)	
		for iter.Next() {
			if err := t.db.Delete(iter.Key(), nil); err != nil {
				log.Fatalln("error in leveldb: ", err)
			}
		}
		reply.R = errors.OK
	case "stat":
		if Debug {
			log.Printf("(uid,gid)=%v,%v stat %v", operation.Uid, operation.Gid, operation.Path)
		}
		if !FileExisted(operation.Path, t) {
			reply.R = errors.NO_SUCH_FILEDIR
			break
		}
		byteArray, err := t.db.Get([]byte(utils.DirentTransfer(operation.Path)), nil)
		if err != nil {
			log.Fatalln("leveldb error!")
		}
		reply.R = errors.OK
		reply.Info = access.ByteArray2FileAccess(byteArray).GetString() 
	case "ls":
		if Debug {
			log.Printf("(uid,gid)=%v,%v ls %v", operation.Uid, operation.Gid, operation.Path)
		}
		prefixL := operation.Path[:len(operation.Path) - 1] + "\\"
		prefixR := strings.Replace(prefixL, "\\", "]", 1)
		iter := t.db.NewIterator(
			&util.Range {
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
	}

	return nil
}

func main() {
	mdmsHome := utils.Home() + "/.mdms/"
	// read passwd file get user list
	UserMap = access.LoadUserConfig(mdmsHome + "passwd")
	// read group file get group info
	GroupMap = access.LoadGroupConfig(mdmsHome + "group", UserMap)

	addr := os.Args[1]
	id := utils.GetID(addr)
	idStr := strconv.Itoa(id)

	// check if previous db store exist & delete
	dbPath := mdmsHome + "fmsdb" + idStr
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

	levelDB = &LevelDB{db: db}
	server := rpc.NewServer()
	server.Register(levelDB)

	l, e := net.Listen("tcp", addr)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	log.Println("ready serving!")
	conCount := 0
	for {
		conn, err := l.Accept()
		if err != nil {
			log.Fatal("Accept Error: ", err)
			continue
		}
		conCount++
		log.Printf("now we have %v clients connected\n", conCount)
		go server.ServeConn(conn)
	}
}

