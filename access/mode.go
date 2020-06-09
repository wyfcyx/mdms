package access

const (
	R uint16 = 4
	W uint16 = 2
	X uint16 = 1
	F uint16 = 0
)

func Ugo2Mode (u uint16, g uint16, o uint16) uint16 {
	return u * 64 + g * 8 + o
}

func Mode2Ugo(mode uint16) (uint16, uint16, uint16) {
	return (mode >> 6) & 7, (mode >> 3) & 7, mode & 7
}

func SingleMode2String(smode uint16) string {
	s := ""
	if (smode & R > 0) {
		s += "r"
	} else {
		s += "-"
	}
	if (smode & W > 0) {
		s += "w"
	} else {
		s += "-"
	}
	if (smode & X > 0) {
		s += "x"
	} else {
		s += "-"
	}
	return s
}

func Mode2String(mode uint16) string {
	u, g, o := Mode2Ugo(mode)
	return SingleMode2String(u) + SingleMode2String(g) + SingleMode2String(o)
}
