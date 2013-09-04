package main

import (
    "net/tomp2p/peers"
    "fmt"
)

func main() {
    fmt.Printf("hello, world\n")
    n,e := tomp2p.NewFromString("0x1114")
    fmt.Printf("%s, %s\n",n.String(),e);
}
