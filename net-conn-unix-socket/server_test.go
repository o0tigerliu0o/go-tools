package main

import (
	"fmt"
	"testing"
	"time"
)

func TestServer(t *testing.T) {
	//声明unixsocket
	us := NewUnixSocket("/go-tools/net-conn-socket/us.socket")
	//设置服务端接收处理
	us.SetContextHandler(func(context string) string {
		fmt.Println(context)
		now := time.Now().String() + "s"
		return now
	})
	//开始服务
	go us.StartServer()
	time.Sleep(time.Second * 600)
}

// 向socket文件中传输信息：echo "this is socket conn" | socat - /go-tools/net-conn-socket/us.socket
