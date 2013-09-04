package main

import (
	"fmt"
	"net"
	"sync"
)

func createTCP() {
	tcpAddr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:4002")
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		//most likely a connection reset by peer
		fmt.Printf("! %s\n",err)
		return
	}
	conn.SetLinger(0)
	conn.Close()
}

func main() {
	fmt.Printf("Testing open/close in go -> client\n")
	
	/*for i := 0; i < 200000; i++ {
		createTCP()
	}*/
	
	var wg sync.WaitGroup
	var connections = 20
	var rounds = 4000
	for i := 0; i < rounds; i++ {
		wg.Add(connections)
		for j := 0; j < connections; j++ {
			go func () { 
				createTCP()
				wg.Done()
			} ()
		}
		wg.Wait()
		fmt.Printf(".")	
	}
	
}
