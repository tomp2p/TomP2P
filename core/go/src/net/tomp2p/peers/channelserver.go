package tomp2p

import (
	"fmt"
	"net"
)

func StartupTCP() {
	tcp, err := net.ResolveTCPAddr("tcp", "127.0.0.1:4002")
	ln, err := net.ListenTCP("tcp", tcp)
	if err != nil {
		return
	}
	for {
		conn, err := ln.AcceptTCP()
		if err != nil {
			// handle error
			fmt.Printf("Error %s\n", err)
			continue
		}
		conn.SetLinger(0)
		//fmt.Printf(".")
		//go handleConnection(conn)
	}
}
