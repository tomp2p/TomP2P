package tomp2p

import (
	"net"
	"fmt"
)

func CreateTCP() {
	tcp, err := net.ResolveTCPAddr("tcp", "127.0.0.1:4002")
	conn, err := net.DialTCP("tcp", nil, tcp)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		fmt.Printf("Exiting...\n")
		return
	}
	conn.Close()
}
