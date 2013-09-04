package tomp2p

import (
	"math/rand"
	"testing"
)

func TestNumber160String1(t *testing.T) {
	_, err := NewFromString("0x1234567890123456789012345678901234567890")
	if err != nil {
		t.Errorf("The format is still acceptable since smaller than 43")
	}
}

func TestNumber160String2(t *testing.T) {
	_, err := NewFromString("0x12345678901234567890123456789012345678901")
	if err == nil {
		t.Errorf("The format is not acceptable since its larger than 42")
	}
}

func TestNumber160String3(t *testing.T) {
	_, err := NewFromString("001234567890123456789012345678901234567890")
	if err == nil {
		t.Errorf("This should fail, since it does not start with 0x")
	}
}

func TestNumber160String4(t *testing.T) {
	n, err := NewFromString("0x1234567890123456789012345678901234567890")
	if n.String() != "0x1234567890123456789012345678901234567890" {
		t.Errorf("It should look like [0x1234567890123456789012345678901234567890], but it was [%s], error = [%s]", n, err)
	}
}

func TestNumber160String5(t *testing.T) {
	n, err := NewFromString("0x1234")
	if n.String() != "0x1234" {
		t.Errorf("It should look like [0x1234567890123456789012345678901234567890], but it was [%s], error = [%s]", n, err)
	}
}

func TestNumber160StringFixed1(t *testing.T) {
	if Max.String() != "0xffffffffffffffffffffffffffffffffffffffff" {
		t.Errorf("It should look like [0xffffffffffffffffffffffffffffffffffffffff], but it was [%s]", Max.String())
	}
}

func TestNumber160StringFixed2(t *testing.T) {
	if Zero.String() != "0x0" {
		t.Errorf("It should look like [0x0], but it was [%s]", Zero.String())
	}
}

func TestNumber160StringFixed3(t *testing.T) {
	if One.String() != "0x1" {
		t.Errorf("It should look like [0x1], but it was [%s]", One.String())
	}
}

func TestNumber160Random(t *testing.T) {
	r := rand.New(rand.NewSource(42))
	number160 := NewFromRandom(r)
	if number160.String() != "0x5f7ec96310e568979aa5e5083575247cb37afbe" {
		t.Errorf("It should look like [0x5f7ec96310e568979aa5e5083575247cb37afbe], but it was [%s]", number160.String())
	}
}

func BenchmarkXxx(*testing.B) {
	//make random numbers and xor them.
}
