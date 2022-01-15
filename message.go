package stomp

import (
	"github.com/go-stomp/stomp/v3"
)

type Message struct {
	*stomp.Message
}

func (m *Message) String() string {
	return string(m.Body)
}
