package fnf_ch

import (
	"encoding/binary"
	"math"
	"math/rand"
	"net"
	"os"
	"strconv"
	"time"
	"unsafe"
)

func getEnvBool(name string) bool {
	valStr := os.Getenv(name)
	if valStr == "" {
		panic("Missing envvar " + name)
	}
	switch valStr {
	case "1", "true", "True":
		return true
	case "0", "false", "False":
		return false
	default:
		panic("Invalid envvar bool " + name + ", value: " + valStr)
	}
}

func getEnvInt(name string) int {
	valStr := os.Getenv(name)
	if valStr == "" {
		panic("Missing envvar " + name)
	}
	valInt, err := strconv.ParseInt(valStr, 10, 64)
	if err != nil {
		panic("Invalid envvar int " + name + ", value: " + valStr)
	}
	if valInt > math.MaxInt {
		panic(valInt)
	}
	return int(valInt)
}

func getEnvDur(name string) time.Duration {
	valStr := os.Getenv(name)
	if valStr == "" {
		panic("Missing envvar " + name)
	}
	valDur, err := time.ParseDuration(valStr)
	if err != nil {
		panic("Invalid envvar duration " + name + ", value: " + valStr)
	}
	return valDur
}

func getEnvStr(name string) string {
	valStr, ok := os.LookupEnv(name)
	if !ok {
		panic("Missing envvar " + name)
	}
	return valStr
}

func randIp() net.IP {
	r := rand.Uint32()
	ip := make(net.IP, net.IPv4len)
	binary.BigEndian.PutUint32(ip, r)
	return ip
}

func byteSlice2String(bs []byte) string {
	return *(*string)(unsafe.Pointer(&bs))
}
