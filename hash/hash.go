package hash

import (
	"hash/crc32"
)

func Hashing(str string) int {
	return int(crc32.ChecksumIEEE([]byte(str)))
}

