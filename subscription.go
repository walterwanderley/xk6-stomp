package stomp

import "github.com/go-stomp/stomp/v3"

type Subscription struct {
	*stomp.Subscription
}

func (s *Subscription) Read() (*Message, error) {
	msg, err := s.Subscription.Read()
	if err != nil {
		return nil, err
	}
	return &Message{Message: msg}, nil
}
