package access

import (
	"fmt"
	"os"
	"io"
	"log"
	"bufio"
	"strings"
	"strconv"
)

// load user config from local file
// @path: path of the local user config
// return: a map from username to an uint32(uid, gid) 
func LoadUserConfig(path string) map[string]uint32 {
	// username -> (uid << 16) | gid
	m := make(map[string]uint32)
	f, err := os.Open(path)
	if err != nil {
		log.Fatalln("error when opening user config: ", err)
	}
	defer f.Close()
	r := bufio.NewReader(f)
	for {
		buf, err := r.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				break
			} else {
				log.Fatalln("error when reading user config: ", err)
			}
		}
		// format: <username>:<uid>:<gid>
		l := string(buf)
		// strip '\n'
		if l[len(l) - 1] == '\n' {
			l = l[:len(l) - 1]
		}
		pl := strings.IndexByte(l, ':')	
		pr := strings.LastIndexByte(l, ':')
		username := l[:pl]
		uidInt, err := strconv.Atoi(l[pl + 1:pr])
		if err != nil {
			log.Fatalln("error when parsing user config: ", err)
		}
		gidInt, err := strconv.Atoi(l[pr + 1:])
		if err != nil {
			log.Fatalln("error when parsing user config: ", err)
		}
		if uidInt < 0 || uidInt >= 65536 || gidInt < 0 || gidInt >= 65536 {
			log.Fatalln("(uid, gid) out of range")
		}
		uid, gid := uint32(uidInt), uint32(gidInt)
		var uidGid uint32 = (uid << 16) + gid
		m[username] = uidGid
	} 
	return m
}

func ViewUserConfig(userMap map[string]uint32) {
	fmt.Println("userMap:")
	for k, v := range(userMap) {
		fmt.Printf("username=%v (uid,gid)=%v,%v\n", k, v >> 16, v & 65535)
	}
}

// load group config from local file
// @path: path of group config file
// return: a map from uid to []gid
func LoadGroupConfig(path string, userMap map[string]uint32) map[uint16][]uint16 {
	f, err := os.Open(path)
	if err != nil {
		log.Fatalln("error when opening group config file: ", err)
	}
	defer f.Close()
	r := bufio.NewReader(f)
	groupMap := make(map[uint16][]uint16)
	for {
		buf, err := r.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				break
			} else {
				log.Fatalln("error when reading group config file: ", err)
			}
		}
		l := string(buf)
		if l[len(l) - 1] == '\n' {
			l = l[:len(l) - 1]
		}
		// format: <group_name>:<gid>:<user_list>
		pl := strings.IndexByte(l, ':')
		pr := strings.LastIndexByte(l, ':')
		gidInt, err := strconv.Atoi(l[pl + 1:pr])
		if err != nil {
			log.Fatalln("error when parsing group config: ", err)
		}
		gid := uint16(gidInt)
		userList := l[pr + 1:]
		// format: userList = <user1>,<user2>,...
		userSplit := strings.Split(userList, ",")
		for _, user := range(userSplit) {
			if uidGid, ok := userMap[user]; ok {
				uid := uint16(uidGid >> 16)
				// fetch previous gid list
				if gidList, ok := groupMap[uid]; ok {
					groupMap[uid] = append(gidList, gid)
				} else {
					var gidList []uint16
					gidList = append(gidList, gid)
					groupMap[uid] = gidList
				}
			} else {
				log.Fatalf("user %v not exist in user file", user)
			}
		}
	}
	return groupMap
}

func GetUserByUid(userMap map[string]uint32, uid uint16) string {
	for k, v := range(userMap) {
		if uint16(v >> 16) == uid {
			return k
		}
	}
	log.Fatalln("error when getting user by uid: not found")
	return ""
}

func ViewGroupConfig(userMap map[string]uint32, groupMap map[uint16][]uint16) {
	fmt.Println("groupMap:")
	for k, v := range(groupMap) {
		fmt.Printf("uid=%v(%v) gidList=%v\n", k, GetUserByUid(userMap, k), v)
	}
}
