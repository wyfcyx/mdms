package main

import (
    "net"
    "fmt"
    "strconv"
    "net/rpc"
	"os"
	"strings"
	"log"
)

type Operation struct {
	Uid uint16
	Gid uint16
	Command string
	Path string
	Args []string
	PairList []uint16
}

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

type Reply struct {
	R int
	Info string
	MDirAccess DirAccess
}

func GetFatherDirectory(path string) string {
	for i := len(path) - 2; i >= 0; i-- {
		if path[i] == '/' {
			return path[:i + 1]
		}
	}
	return path
}

// using "pass" command here
func CheckOpacity(path string, uid uint16, gid uint16, client *rpc.Client) (bool, string, DirAccess) {
	operation := Operation{uid, gid, "pass", path, nil, nil}
	var reply Reply
	if err := client.Call("LevelDB.Query", operation, &reply); err != nil {
		log.Fatalln("error during rpc: ", err)
	}
	if reply.R == 0 {
		// passed
		return true, "", reply.MDirAccess
	} else {
		// not passed
		return false, reply.Info, DirAccess{}
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
	fmt.Println("ready for local client")
    c, err := l.Accept()
    if err != nil {
        fmt.Println(err)
        return
    }
    defer c.Close()
	fmt.Println("local client connected")

	// send remote request to server 
    client, err := rpc.Dial("tcp", "10.1.0.20:1234")
    if err != nil {
        fmt.Println(err)
        return
    }
	fmt.Println("connected to remote server")
	defer client.Close()

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
	var uid uint16
	var gid uint16
	uid = uint16(temp)
	gid = uint16(temp)


	// TODO: configure uid && gid on local client

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
			passed, info, dirAccess := CheckOpacity(GetFatherDirectory(path), uid, gid, client)
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
				err = client.Call("LevelDB.Query", operation, &reply)
				// TODO: select a correct server using consistent hashing
				if err != nil {
					log.Fatalln("error during rpc: ", err)
				} else if reply.R < 0 {
					fmt.Printf("operation failed: reply = %v info = %v", reply.R, reply.Info)
				}
				result = "OK"
			} else {
				fmt.Println("access denied")
				result = info
			}
		case "ls":
			 // access validation: check opacity from root to path's
			passed, info, dirAccess := CheckOpacity(path, uid, gid, client)
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
				err := client.Call("LevelDB.Query", operation, &reply)
				// TODO: select a specific server using consistent hashing
				if err != nil {
					log.Fatalln("error during rpc: ", err)
				} else if reply.R < 0 {
					fmt.Printf("operation failed: reply = %v info = %v", reply.R, reply.Info)
				}
				result = reply.Info
			} else {
				fmt.Println("access denied")
				result = info
			}
		case "stat":
			// access validation: check opacity from root to path's parent
			passed, info, _ := CheckOpacity(GetFatherDirectory(path), uid, gid, client)
			if passed {
				operation := Operation {
					uid,
					gid,
					"stat",
					path,
					nil,
					nil}
				var reply Reply		
				err := client.Call("LevelDB.Query", operation, &reply)
				// TODO: select a specific server
				if err != nil {
					log.Fatalln("error during rpc: ", err)
				} else if reply.R < 0 {
					fmt.Printf("operation failed: reply = %v info = %v", reply.R, reply.Info)
				}
				result = reply.MDirAccess.GetString()
			} else {
				fmt.Println("access denied")
				result = info
			}
		}
		
        if _, err := c.Write([]byte(result)); err != nil {
			log.Fatalln("error when sending result to client")
        }
    } 
}
