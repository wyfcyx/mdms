package main

import (
	"fmt"
	"net"
	"bufio"
	"os"
	"strings"
	//"io"
	"strconv"
)

func isDir(path string) bool {
	return path[0] == '/' && path[len(path) - 1] == '/'
}

func isFile(path string) bool {
	return path[0] == '/' && path[len(path) - 1] != '/'
}

func commandValidation(command string) bool {
	split := strings.Split(command, " ")
	c_type, path := split[0], split[1]
	if c_type == "nop" {
		return false
	}
	//fmt.Printf("c_type = %v path = %v\n", c_type, path)
	switch c_type {
	case "mkdir", "ls":
		if !isDir(path) {
			fmt.Println("Invalid directory path")
			return false	
		}
	default:
	}
	return true
}

func commandHandle(command string, conn *net.Conn) {
	if commandValidation(command) {
		(*conn).Write([]byte(command))
		result := make([]byte, 1024)
		(*conn).Read([]byte(result))
		fmt.Println("result = ", string(result))
	}
}

func main() {
	local_rank, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("parsing error: ", err)
		return
	}
	if err != nil {
		fmt.Println("parsing error: ", err)
	}
	conn, err := net.Dial("tcp4", "localhost:" + strconv.Itoa(6789 + local_rank))
	// write uid = gid
	conn.Write([]byte(os.Args[2]))
	if err != nil {
		fmt.Println(err)
		return
	}
	defer conn.Close()
	fmt.Println("connected!")

	// read initialization commands and execute
	/*
	f, err := os.Open("init")
    if err != nil {
        fmt.Println("error when opening file: ", err)
        return
    }
    r := bufio.NewReader(f)
    for {
        buf, err := r.ReadBytes('\n')
        if err != nil {
            if err == io.EOF {
                break
            } else {
                fmt.Println("error when reading file: ", err)
                os.Exit(1)
            }
        }
        buf = buf[:len(buf) - 1]
		command := string(buf)
        fmt.Println("initial command: ", command)
		commandHandle(command, &conn)
    }
	f.Close()
	*/

	// interactive console
	input := bufio.NewScanner(os.Stdin)	
	for {
		fmt.Printf(">> ")
		input.Scan()
		line := input.Text()
		if line == "exit" {
			break
		}
		commandHandle(line, &conn)
	}
}
