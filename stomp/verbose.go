package stomp

import (
	"io"
	"log"
)

type VerboseReadWriteClose struct {
	io.ReadWriteCloser
}

func (v *VerboseReadWriteClose) Read(p []byte) (int, error) {
	n, err := v.ReadWriteCloser.Read(p)
	if err != nil {
		log.Println("READ-ERR:", err.Error())
	} else if n > 0 {
		log.Println("READ:", string(p[0:n]))
	}
	return n, err
}

func (v *VerboseReadWriteClose) Write(p []byte) (int, error) {
	n, err := v.ReadWriteCloser.Write(p)
	if err != nil {
		log.Println("WRITE-ERR:", err.Error())
	} else if n > 0 {
		log.Println("WRITE:", string(p[0:n]))
	}
	return n, err
}
