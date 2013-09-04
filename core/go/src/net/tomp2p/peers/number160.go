package tomp2p

import (
	"errors"
	"fmt"
	"strings"
	"strconv"
	"bytes"
	"math/rand"
	"encoding/binary"
)

const (
	Bits = 160
	ArraySize = Bits / 8
)

var (
	Max = Number160{^uint64(0),^uint64(0),^uint32(0)}
	Zero = Number160{0, 0, 0}
	One = Number160{0, 0, 1}
)

type Number160 struct {
    u1 uint64
    u2 uint64
    u3 uint32
}

//create a Number160 from a string, e.g. 0x123456789abcdef
func NewFromString(s string) (number160 *Number160, err error) {
	if len(s) > 42 {
    	err = errors.New(fmt.Sprintf("Can only deal with strings of size smaller or equal than 42. Your string has %i", len(s)))
    	return
    }
    if strings.HasPrefix(s, "0x") == false {
    	err = errors.New(fmt.Sprintf("[%s] is not in hexadecimal form. Decimal form is not supported yet", s))
    	return
    }
    number160 = &Number160{0,0,0}
    //skip the first to bytes since its 0x
    
    length := len(s)
    if length == 2 {
    	//numbers are set to 0, we can quit here
    	return
    }
    
    start := length - 8
    if start < 2 {	
    	start = 2
    }
    
    i, err2 := strconv.ParseUint(s[start:length],16,32)
    if err2 != nil {
    	err = errors.New(fmt.Sprintf("[%s] cannot be parsed: %s ", s, err2))
    	return
    }
    number160.u3 = uint32(i)
    if start == 2 {
    	return
    }
    
    length = length - 8
    start = start - 16
    if start < 2 {	
    	start = 2
    }
    
    number160.u2, err2 = strconv.ParseUint(s[start:length],16,64)
    if err2 != nil {
    	err = errors.New(fmt.Sprintf("[%s] cannot be parsed: %s ", s, err2))
    	return
    }
    if start == 2 {
    	return
    }
    
    length = length - 16
    start = start - 16
    if start < 2 {	
    	start = 2
    }
    
    number160.u1, err2 = strconv.ParseUint(s[start:length],16,64)
    if err2 != nil {
    	err = errors.New(fmt.Sprintf("[%s] cannot be parsed: %s ", s, err2))
    	return
    }
    
    return
}

func New() *Number160 {
    return &Number160{0,0,0}
}

func NewFromInt(u uint32) *Number160 {
    return &Number160{0,0,u}
}

func NewFromInts(u1 uint64, u2 uint64, u3 uint32) *Number160 {
    return &Number160{u1,u2,u3}
}

func NewFromLong(u uint64) *Number160 {
    return &Number160{0,u<<32,uint32(u)}
}

func NewFromRandom(r *rand.Rand) *Number160 {
	// there is no rand.Uint64(). Seems that it was forgotten:
	// https://groups.google.com/forum/#!topic/golang-nuts/Kle874lT1Eo
	u1 := uint64(r.Uint32())<<32 + uint64(r.Uint32())
	u2 := uint64(r.Uint32())<<32 + uint64(r.Uint32())
    return &Number160{u1,u2,r.Uint32()}
}

func NewFromBuffer(buffer *bytes.Buffer) (number160 *Number160, err error) {
	//var number160 Number160
	err = binary.Read(buffer, binary.LittleEndian, &number160)
    if err != nil {
        err = errors.New(fmt.Sprintf("Error in parsing [%s]", err))
        return
    }
    return
}

//func (n *Number160) ToBuffer (buffer *bytes.Buffer) err error {
//	err = binary.Write(buffer, binary.LittleEndian, &n)
//} 

//newfrom byte buffer

func (n *Number160) Xor(number160 *Number160) *Number160 {
	return &Number160{n.u1 ^ number160.u1, n.u2 ^ number160.u2, n.u3 ^ number160.u3}
}

func (n *Number160) String() string {
	var buffer bytes.Buffer
	buffer.WriteString("0x");
	if n.u1 != 0 {
		buffer.WriteString(strconv.FormatUint(n.u1,16));
	}
	if n.u2 != 0 {
		buffer.WriteString(strconv.FormatUint(n.u2,16));
	}
	buffer.WriteString(strconv.FormatUint(uint64(n.u3),16));
	return buffer.String()
}