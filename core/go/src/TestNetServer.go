package main

import (
    "net"
    "fmt"
    "sync"
)

func startupTCP() {
	tcpAddr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:4002")
	ln, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		return
	}
	for {
		_ , err := ln.AcceptTCP()
		if err != nil {
			fmt.Printf("Error %s\n", err)
			continue
		}
		//conn.SetLinger(0)
		fmt.Printf(",")
	}
}

func main() {
	var w sync.WaitGroup
	w.Add(1)
    fmt.Printf("Testing open/close in go -> server\n")
    //start server
    go startupTCP()
    w.Wait()
}
