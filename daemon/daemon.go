package main

import (
    "net"
    "fmt"
    "strconv"
    "net/rpc"
	"os"
	"strings"
	"log"
	"bufio"

	"github.com/wyfcyx/mdms/access"
	"github.com/wyfcyx/mdms/utils"
	"github.com/wyfcyx/mdms/mrpc"
	"github.com/wyfcyx/mdms/errors"
	"github.com/wyfcyx/mdms/hash"
)

var (
	N int
	M int
	DMSClients []*rpc.Client
	FMSClients []*rpc.Client	
)

const (
	Debug bool = false
)
// using "pass" command here
func CheckOpacity(path string, uid uint16, gid uint16) (bool, string, access.DirAccess) {
	operation := mrpc.DOperation{uid, gid, "pass", path, nil, nil}
	dclient := DMSClients[hash.Hashing(path) % N]
	var reply mrpc.DReply
	if err := dclient.Call("LevelDB.Query", operation, &reply); err != nil {
		log.Fatalln("error during rpc: ", err)
	}
	if reply.R == 0 {
		// passed
		return true, "", reply.MDirAccess
	} else {
		// not passed
		return false, errors.ErrorString(reply.R), access.DirAccess{}
	}
}

func GetInt(r *bufio.Reader) int {
	buf, err := r.ReadBytes('\n')
	if err != nil {
		log.Fatalln("error when reading nodes: ", err)
	}
	str := string(buf)
	str = str[:len(str) - 1]
	n, err := strconv.Atoi(str)
	if err != nil {
		log.Fatalln("error when reading nodes: ", err)
	}
	return n
}

func main() {
	local_rank, err := strconv.Atoi(os.Args[1]) 
	if err != nil {
		fmt.Println(err)
		return
	}
	//fmt.Println(os.Args[1])

	// receive local command from c
    l, err := net.Listen("tcp4", "localhost:" + strconv.Itoa(6789 + local_rank))
    if err != nil {
        fmt.Println(err)
        return
    }
    defer l.Close()
	log.Println("ready for local client")
    c, err := l.Accept()
    if err != nil {
        fmt.Println(err)
        return
    }
    defer c.Close()
	log.Println("local client connected")

	mdmsHome := utils.Home() + "/.mdms/"
	f, err := os.Open(mdmsHome + "nodes")
	if err != nil {
		log.Fatalln("error when opening nodes: ", err)
	}
	r := bufio.NewReader(f)
	N = GetInt(r)
	DMSClients = make([]*rpc.Client, N)
	for i := 0; i < N; i++ {
		buf, err := r.ReadBytes('\n')
		if err != nil {
			log.Fatalln("error when reading nodes: ", err)
		}
		str := string(buf)
		str = str[:len(str) - 1]
		DMSClients[i], err = rpc.Dial("tcp", str)
		if err != nil {
			log.Fatalln("error when connecting to DMS: ", err)
		}
		defer DMSClients[i].Close()
	}
	M = GetInt(r)
	FMSClients = make([]*rpc.Client, M)
	for i := 0; i < M; i++ {
		buf, err := r.ReadBytes('\n')
		if err != nil {
			log.Fatalln("error when reading nodes: ", err)
		}
		str := string(buf)
		str = str[:len(str) - 1]
		FMSClients[i], err = rpc.Dial("tcp", str)
		if err != nil {
			log.Fatalln("error when connecting to FMS: ", err)
		}
		defer FMSClients[i].Close()
	}
	
	// read uid from client
	uidArray := make([]byte, 1024)
	if _, err := c.Read(uidArray); err != nil {
		fmt.Println("error when receiving uid: ", err)
		return
	}
	for i := 0; i < len(uidArray); i++ {
		if uidArray[i] == 0 {
			uidArray = uidArray[:i]
			break
		}
	}
	temp, err := strconv.Atoi(string(uidArray))
	if err != nil {
		fmt.Println("error when parsing uid: ", err)
		return
	}

	// initialize uid, gid
	var uid uint16
	var gid uint16
	uid = uint16(temp)
	gid = uint16(temp)

	// TODO: interact with manager
	// validate the uid, query related username  
	// send the info back to client & initialize it

    for {
		// receive command from local client
        recv := make([]byte, 1024)
        _, err := c.Read(recv)    
        if err != nil {
            fmt.Println(err)
            return
        }
        //fmt.Printf("Received message = " + string(recv[:]) + "\n")
		
		// parse the command into c_type and path
		e := 0
		for i := 0; i < len(recv); i++ {
			if recv[i] == 0 {
				e = i
				break
			}
		}
		command := string(recv[:e])
		split := strings.Split(command, " ")
		c_type := split[0]
		path := split[1]
		args := split[2:]
		
		// fix wrong path
		switch c_type {
		case "mkdir", "ls", "rmdir", "access":
			if !utils.IsDir(path) {
				path += "/"
			}
		case "create", "delete":
			if !utils.IsFile(path) {
				path = path[:len(path) - 1]
			}
		}
		if Debug {
			log.Printf("%v %v %v\n", c_type, path, args)
		}

		result := ""
		// send back to local client

		// turn the command into operation on server
		switch c_type {
		case "mkdir":
			// access validation: check opacity from root to path's father dir
			passed, _, dirAccess := CheckOpacity(utils.GetFatherDirectory(path), uid, gid)
			if passed && dirAccess.DirAccessCheck(uid, gid, access.W) {
				// we need write access as well
				// now, send request to correct node
				operation := mrpc.DOperation {
					uid,
					gid,
					"mkdir",
					path,
					args,
					dirAccess.PairList}
				var reply mrpc.DReply
				dclient := DMSClients[hash.Hashing(operation.Path) % N]
				err = dclient.Call("LevelDB.Query", operation, &reply)
				if err != nil {
					log.Fatalln("error during rpc: ", err)
				} else if reply.R < 0 {
					log.Printf("operation failed: reply = %v info = %v", reply.R, reply.Info)
				}
				result = errors.ErrorString(reply.R)
			} else {
				result = errors.ErrorString(errors.ACCESS_DENIED) 
			}
		case "rmdir": // request for all dms/fms
			passed, _, dirAccess := CheckOpacity(utils.GetFatherDirectory(path), uid, gid)
			if passed && dirAccess.DirAccessCheck(uid, gid, access.W) {
				// distribute to every dms
				doperation := mrpc.DOperation {
					uid,
					gid,
					"rmdir",
					path,
					nil,
					nil,
				}
				dreplys := make([]mrpc.DReply, N)
				dCallChans := make([]chan *rpc.Call, N)
				for i := 0; i < N; i++ {
					dclient := DMSClients[hash.Hashing(doperation.Path) % N]
					dCallChans[i] = make(chan *rpc.Call, 1)
					dclient.Go("LevelDB.Query", doperation, &dreplys[i], dCallChans[i])
				}

				// distribute to every fms
				foperation := mrpc.FOperation {
					uid,
					gid,
					"rmdir",
					path,
					nil,
				}
				freplys := make([]mrpc.FReply, M)
				fCallChans := make([]chan *rpc.Call, M)
				for i := 0; i < M; i++ {
					fclient := FMSClients[hash.Hashing(foperation.Path) % M]
					fCallChans[i] = make(chan *rpc.Call, 1)
					fclient.Go("LevelDB.Query", foperation, &freplys[i], fCallChans[i])
				}

				// wait for all tasks to complete
				for i := 0; i < N; i++ {
					<-dCallChans[i]
					if dreplys[i].R < 0 {
						log.Fatalln("error when rmdir")
					}
				}
				for i := 0; i < M; i++ {
					<-fCallChans[i]
					if freplys[i].R < 0 {
						log.Fatalln("error when rmdir")
					}
				}
				result = errors.ErrorString(errors.OK)
			} else {
				result = errors.ErrorString(errors.ACCESS_DENIED)
			}
		case "ls":
			// access validation: check opacity from root to path's
			passed, _, dirAccess := CheckOpacity(path, uid, gid)
			if passed && dirAccess.DirAccessCheck(uid, gid, access.R) {
				// we need read access as well to list
				// distribute tasks to every dms
				doperation := mrpc.DOperation {
					uid,
					gid,
					"ls",
					path,
					nil,
					nil,
				}
				dreplys := make([]mrpc.DReply, N)
				dCallChans := make([]chan *rpc.Call, N)
				for i := 0; i < N; i++ {
					dclient := DMSClients[hash.Hashing(doperation.Path) % N]
					dCallChans[i] = make(chan *rpc.Call, 1)
					dclient.Go("LevelDB.Query", doperation, &dreplys[i], dCallChans[i])
				}

				// distribute tasks to every fms
				foperation := mrpc.FOperation {
					uid,
					gid,
					"ls",
					path,
					nil,
				}
				freplys := make([]mrpc.FReply, M)
				fCallChans := make([]chan *rpc.Call, M)
				for i := 0; i < M; i++ {
					fclient := FMSClients[hash.Hashing(foperation.Path) % M]
					fCallChans[i] = make(chan *rpc.Call, 1)
					fclient.Go("LevelDB.Query", foperation, &freplys[i], fCallChans[i])
				}

				// wait for every task to complete
				for i := 0; i < N; i++ {
					<-dCallChans[i]
					if dreplys[i].R < 0 {
						log.Fatalln("error when ls: ", errors.ErrorString(dreplys[i].R))
					}
					result += dreplys[i].Info
				}
				for i := 0; i < M; i++ {
					<-fCallChans[i]
					if freplys[i].R < 0 {
						log.Fatalln("error when ls: ", errors.ErrorString(freplys[i].R))
					}
					result += freplys[i].Info
				}
			} else {
				result = errors.ErrorString(errors.ACCESS_DENIED)
			}
		case "stat":
			// access validation: check opacity from root to path's parent
			passed, _, _ := CheckOpacity(utils.GetFatherDirectory(path), uid, gid)
			if utils.IsDir(path) {
				if passed {
					operation := mrpc.DOperation {
						uid,
						gid,
						"stat",
						path,
						nil,
						nil}
					var reply mrpc.DReply		
					dclient := DMSClients[hash.Hashing(operation.Path) % N]
					err := dclient.Call("LevelDB.Query", operation, &reply)
					if err != nil {
						log.Fatalln("error during rpc: ", err)
					} else if reply.R < 0 {
						fmt.Printf("operation failed: reply = %v info = %v", reply.R, reply.Info)
						result = errors.ErrorString(reply.R)
					} else {
						result = reply.MDirAccess.GetString()
					}
				} else {
					result = errors.ErrorString(errors.ACCESS_DENIED)
				}
			} else {
				if passed {
					// first try file
					operation := mrpc.FOperation {
						uid,
						gid,
						"stat",
						path,
						nil}
					var reply mrpc.FReply
					fclient := FMSClients[hash.Hashing(operation.Path) % M]
					err := fclient.Call("LevelDB.Query", operation, &reply)
					if err != nil {
						log.Fatalln("error during rpc: ", err)
					} else if reply.R < 0 {
						// try directory
						doperation := mrpc.DOperation {
							uid,
							gid,
							"stat",
							path + "/",
							nil,
							nil}
						var dreply mrpc.DReply
						dclient := DMSClients[hash.Hashing(operation.Path) % N]
						err := dclient.Call("LevelDB.Query", doperation, &dreply)
						if err != nil {
							log.Fatalln("error during rpc: ", err)
						} else if dreply.R < 0 {
							result = errors.ErrorString(dreply.R)
						} else {
							result = dreply.MDirAccess.GetString()
						}
					} else {
						result = reply.Info
					}
				} else {
					result = errors.ErrorString(errors.ACCESS_DENIED)
				}
			}
		case "create":
			pass, _, dirAccess := CheckOpacity(utils.GetFatherDirectory(path), uid, gid)
			if pass && dirAccess.DirAccessCheck(uid, gid, access.W) {
				operation := mrpc.FOperation {
					uid,
					gid,
					"create",
					path,
					nil}
				var reply mrpc.FReply
				fclient := FMSClients[hash.Hashing(operation.Path) % M]
				if err := fclient.Call("LevelDB.Query", operation, &reply); err != nil {
					log.Fatalln("error during rpc")
				}
				if reply.R < 0 {
					log.Printf("operation failed: reply = %v info = %v", errors.ErrorString(reply.R), reply.Info)
				}
				result = errors.ErrorString(reply.R)
			} else {
				result = errors.ErrorString(errors.ACCESS_DENIED)
			}
		case "delete":
			pass, _, dirAccess := CheckOpacity(utils.GetFatherDirectory(path), uid, gid)
			if pass && dirAccess.DirAccessCheck(uid, gid, access.W) {
				operation := mrpc.FOperation {
					uid,
					gid,
					"delete",
					path,
					nil}
				var reply mrpc.FReply
				fclient := FMSClients[hash.Hashing(operation.Path) % M]
				if err := fclient.Call("LevelDB.Query", operation, &reply); err != nil {
					log.Fatalln("error during rpc")
				}
				if reply.R < 0 {
					log.Printf("operation failed: reply = %v info = %v", errors.ErrorString(reply.R), reply.Info)
				}
				result = errors.ErrorString(reply.R)
			} else {
				result = errors.ErrorString(errors.ACCESS_DENIED)
			}
		case "access":
			pass, _, _ := CheckOpacity(utils.GetFatherDirectory(path), uid, gid)
			if pass {
				operation := mrpc.DOperation {
					uid,
					gid,
					"access",
					path,
					args,
					nil}
				var reply mrpc.DReply
				dclient := DMSClients[hash.Hashing(operation.Path) % N]
				if err := dclient.Call("LevelDB.Query", operation, &reply); err != nil {
					log.Fatalln("error during rpc: ", err)
				}
				if reply.R < 0 {
					result = errors.ErrorString(reply.R)
				} else {
					result = "OK"
				}
			} else {
				result = errors.ErrorString(errors.ACCESS_DENIED)
			}
		default:
			log.Printf("unhandled command %v\n", c_type)
		}
		
		if result == "" {
			result = "OK"
		}
		fmt.Printf("result = %v\n", result)
        if _, err := c.Write([]byte(result)); err != nil {
			log.Fatalln("error when sending result to client")
        }
    } 
}
