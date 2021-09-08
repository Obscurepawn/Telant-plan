/*
 * @Author: uestc.zyj@gmail.com
 * @Date: 2021-09-08 15:38:35
 * @LastEditTime: 2021-09-08 15:47:02
 * @Description: my util
 * @FilePath: /tinykv/kv/util/my_util.go
 */

package util

import (
	"runtime"
	"unsafe"

	"github.com/pingcap-incubator/tinykv/log"
)

func Str2bytes(s string) []byte {
	x := (*[2]uintptr)(unsafe.Pointer(&s))
	b := [3]uintptr{x[0], x[1], x[1]}
	return *(*[]byte)(unsafe.Pointer(&b))
}

func Bytes2str(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func ShowStack() {
	const size = 64 << 10
	buf := make([]byte, size)
	buf = buf[:runtime.Stack(buf, false)]
	log.Error(Bytes2str(buf))
}

func PanicPack(fnName string) {
	if err := recover(); err != nil {
		ShowStack()
		log.Error("[%v] panic happened:%v", fnName, err)
	}
}
