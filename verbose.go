package stomp

import (
	"io"
	"log"
)

type VerboseReadWriteClose struct {
	rwc io.ReadWriteCloser
}

func (v *VerboseReadWriteClose) Read(p []byte) (int, error) {
	n, err := v.rwc.Read(p)
	if err != nil {
		log.Println("READ-ERR:", err.Error())
	} else if n > 0 {
		log.Println("READ:", string(p[0:n]))
	}
	return n, err
}

func (v *VerboseReadWriteClose) Write(p []byte) (int, error) {
	n, err := v.rwc.Write(p)
	if err != nil {
		log.Println("WRITE-ERR:", err.Error())
	} else if n > 0 {
		log.Println("WRITE:", string(p[0:n]))
	}
	return n, err
}

func (v *VerboseReadWriteClose) Close() error {
	return v.rwc.Close()
}
