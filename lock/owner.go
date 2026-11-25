package lock

import (
	"runtime"
	"strconv"
	"strings"
)

// GoroutineOwner 返回当前 goroutine 的全局唯一标识串(形如
// "host-1231-a1b2c3d4-g1234"),用作 Config.OwnerFunc 即实现「goroutine 绑定」式可重入:
// 同一 goroutine 递归加锁只加深计数,跨 goroutine 仍按互斥处理。
//
// 前缀带进程标识是必要的:goroutine id 仅在本进程内唯一且会被复用,
// 裸 goid 会让多副本间的同号 goroutine 互相误判为同一持有者。
//
// 标识取自 runtime.Stack 首行,单次约 1µs——只在加锁/解锁时调用,不在热路径循环内。
// 提取失败(极端情况下的短缓冲)时退化为实例级唯一串,此时该次调用失去重入能力,
// 但不会误判成他人属主。
func GoroutineOwner() string {
	id, ok := goroutineID()
	if !ok {
		return defaultOwner()
	}
	return procID + "-g" + strconv.FormatUint(id, 10)
}

// goroutineID 解析 runtime.Stack 首行 "goroutine N [status]:" 中的编号。
// 不缓存:goroutine 复用(极端场景)会串号,每次实取。
func goroutineID() (uint64, bool) {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	s := string(buf[:n])
	const prefix = "goroutine "
	if !strings.HasPrefix(s, prefix) {
		return 0, false
	}
	rest := s[len(prefix):]
	idx := strings.IndexByte(rest, ' ')
	if idx <= 0 {
		return 0, false
	}
	id, err := strconv.ParseUint(rest[:idx], 10, 64)
	if err != nil {
		return 0, false
	}
	return id, true
}
