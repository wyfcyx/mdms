package main

import (
    "net"
    "fmt"
    "strconv"
    "net/rpc"
	"os"
	"strings"
	"log"

	"github.com/wyfcyx/mdms/access"
	"github.com/wyfcyx/mdms/utils"
	"github.com/wyfcyx/mdms/mrpc"
	"github.com/wyfcyx/mdms/errors"
)

// using "pass" command here
func CheckOpacity(path string, uid uint16, gid uint16, dclient *rpc.Client) (bool, string, access.DirAccess) {
	operation := mrpc.DOperation{uid, gid, "pass", path, nil, nil}
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

	// send remote request to server 
    dclient, err := rpc.Dial("tcp", "10.1.0.20:1234")
    if err != nil {
        fmt.Println(err)
        return
    }
	log.Println("connected to dms")
	defer dclient.Close()

	fclient, err := rpc.Dial("tcp", "10.1.0.20:1235")
	if err != nil {
		fmt.Println(err)
		return
	}
	log.Println("connected to fms")
	defer fclient.Close()

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
		log.Printf("%v %v %v\n", c_type, path, args)

		result := ""
		// send back to local client

		// turn the command into operation on server
		switch c_type {
		case "mkdir":
			// access validation: check opacity from root to path's father dir
			passed, _, dirAccess := CheckOpacity(utils.GetFatherDirectory(path), uid, gid, dclient)
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
				err = dclient.Call("LevelDB.Query", operation, &reply)
				// TODO: select a correct server using consistent hashing
				if err != nil {
					log.Fatalln("error during rpc: ", err)
				} else if reply.R < 0 {
					log.Printf("operation failed: reply = %v info = %v", reply.R, reply.Info)
				}
				result = errors.ErrorString(reply.R)
			} else {
				result = errors.ErrorString(errors.ACCESS_DENIED) 
			}
		case "rmdir":
			passed, _, dirAccess := CheckOpacity(utils.GetFatherDirectory(path), uid, gid, dclient)
			if passed && dirAccess.DirAccessCheck(uid, gid, access.W) {
				doperation := mrpc.DOperation {
					uid,
					gid,
					"rmdir",
					path,
					nil,
					nil}
				var dreply mrpc.DReply
				dCallChan := make(chan *rpc.Call, 1)
				dclient.Go("LevelDB.Query", doperation, &dreply, dCallChan)
				
				foperation := mrpc.FOperation {
					uid,
					gid,
					"rmdir",
					path,
					nil,
				}	
				var freply mrpc.FReply
				fCallChan := make(chan *rpc.Call, 1)
				fclient.Go("LevelDB.Query", foperation, &freply, fCallChan)

				<-dCallChan
				<-fCallChan

				if dreply.R < 0 || freply.R < 0 {
					log.Fatalln("error when rmdir")
				}

				result = errors.ErrorString(errors.OK)
			} else {
				result = errors.ErrorString(errors.ACCESS_DENIED)
			}
		case "ls":
			// access validation: check opacity from root to path's
			passed, _, dirAccess := CheckOpacity(path, uid, gid, dclient)
			if passed && dirAccess.DirAccessCheck(uid, gid, access.R) {
				// we need read access as well to list
				doperation := mrpc.DOperation {
					uid,
					gid,
					"ls",
					path,
					nil,
					nil}
				var dreply mrpc.DReply
				dCallChan := make(chan *rpc.Call, 1)
				dclient.Go("LevelDB.Query", doperation, &dreply, dCallChan)

				foperation := mrpc.FOperation {
					uid,
					gid,
					"ls",
					path,
					nil}
				var freply mrpc.FReply
				fCallChan := make(chan *rpc.Call, 1)
				fclient.Go("LevelDB.Query", foperation, &freply, fCallChan)

				<-dCallChan
				<-fCallChan

				// TODO: select a specific server using consistent hashing
				if dreply.R < 0 {
					log.Printf("operation failed: reply = %v info = %v", errors.ErrorString(dreply.R), dreply.Info)
					result = errors.ErrorString(dreply.R)
					break
				} else {
					result += dreply.Info
				}

				if freply.R < 0 {
					log.Printf("operation failed: reply = %v info = %v", errors.ErrorString(freply.R), freply.Info)
					result = errors.ErrorString(freply.R)
					break
				} else {
					result += freply.Info
				}
			} else {
				result = errors.ErrorString(errors.ACCESS_DENIED)
			}
		case "stat":
			// access validation: check opacity from root to path's parent
			passed, _, _ := CheckOpacity(utils.GetFatherDirectory(path), uid, gid, dclient)
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
					err := dclient.Call("LevelDB.Query", operation, &reply)
					// TODO: select a specific server
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
			pass, _, dirAccess := CheckOpacity(utils.GetFatherDirectory(path), uid, gid, dclient)
			if pass && dirAccess.DirAccessCheck(uid, gid, access.W) {
				operation := mrpc.FOperation {
					uid,
					gid,
					"create",
					path,
					nil}
				var reply mrpc.FReply
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
			pass, _, dirAccess := CheckOpacity(utils.GetFatherDirectory(path), uid, gid, dclient)
			if pass && dirAccess.DirAccessCheck(uid, gid, access.W) {
				operation := mrpc.FOperation {
					uid,
					gid,
					"delete",
					path,
					nil}
				var reply mrpc.FReply
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
			pass, _, _ := CheckOpacity(utils.GetFatherDirectory(path), uid, gid, dclient)
			if pass {
				operation := mrpc.DOperation {
					uid,
					gid,
					"access",
					path,
					args,
					nil}
				var reply mrpc.DReply
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
