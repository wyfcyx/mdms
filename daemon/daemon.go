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
)

type Operation struct {
	Uid uint16
	Gid uint16
	Command string
	Path string
	Args []string
	PairList []uint16
}

type Reply struct {
	R int
	Info string
	MDirAccess access.DirAccess
}

// using "pass" command here
func CheckOpacity(path string, uid uint16, gid uint16, dclient *rpc.Client) (bool, string, access.DirAccess) {
	operation := Operation{uid, gid, "pass", path, nil, nil}
	var reply Reply
	if err := dclient.Call("LevelDB.Query", operation, &reply); err != nil {
		log.Fatalln("error during rpc: ", err)
	}
	if reply.R == 0 {
		// passed
		return true, "", reply.MDirAccess
	} else {
		// not passed
		return false, reply.Info, access.DirAccess{}
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
	/*
	fclient, err := rpc.Dial("tcp", "10.1.0.20:1235")
	if err != nil {
		fmt.Println(err)
		return
	}
	log.Println("connected to fms")
	*/


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
        fmt.Printf("Received message = " + string(recv[:]) + "\n")
		
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
		
		result := ""
		// send back to local client

		// turn the command into operation on server
		switch c_type {
		case "mkdir":
			// access validation: check opacity from root to path's father dir
			passed, _, dirAccess := CheckOpacity(utils.GetFatherDirectory(path), uid, gid, dclient)
			if passed && dirAccess.DirAccessCheck(uid, gid, 2) {
				// we need write access as well
				// now, send request to correct node
				operation := Operation {
					uid,
					gid,
					"mkdir",
					path,
					nil,
					// TODO: select another mode other than default: 755
					dirAccess.PairList}
				var reply Reply
				err = dclient.Call("LevelDB.Query", operation, &reply)
				// TODO: select a correct server using consistent hashing
				if err != nil {
					log.Fatalln("error during rpc: ", err)
				} else if reply.R < 0 {
					fmt.Printf("operation failed: reply = %v info = %v", reply.R, reply.Info)
				}
				result = "OK"
			} else {
				fmt.Println("access denied")
				//result = info + " access denied"
				result = "access denied"
			}
		case "ls":
			 // access validation: check opacity from root to path's
			passed, _, dirAccess := CheckOpacity(path, uid, gid, dclient)
			if passed && dirAccess.DirAccessCheck(uid, gid, 4) {
				// we need read access as well to list
				operation := Operation {
					uid,
					gid,
					"ls",
					path,
					nil,
					nil}
				var reply Reply
				err := dclient.Call("LevelDB.Query", operation, &reply)
				// TODO: select a specific server using consistent hashing
				if err != nil {
					log.Fatalln("error during rpc: ", err)
				} else if reply.R < 0 {
					fmt.Printf("operation failed: reply = %v info = %v", reply.R, reply.Info)
				}
				result = reply.Info
			} else {
				fmt.Println("access denied")
				//result = info
				result = "access denied"
			}
		case "stat":
			// access validation: check opacity from root to path's parent
			passed, _, _ := CheckOpacity(utils.GetFatherDirectory(path), uid, gid, dclient)
			if passed {
				operation := Operation {
					uid,
					gid,
					"stat",
					path,
					nil,
					nil}
				var reply Reply		
				err := dclient.Call("LevelDB.Query", operation, &reply)
				// TODO: select a specific server
				if err != nil {
					log.Fatalln("error during rpc: ", err)
				} else if reply.R < 0 {
					fmt.Printf("operation failed: reply = %v info = %v", reply.R, reply.Info)
				}
				result = reply.MDirAccess.GetString()
			} else {
				fmt.Println("access denied")
				//result = info
				result = "access denied"
			}
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
