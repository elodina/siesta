package siesta

import "net"

type Selector interface {
	connect(string, string, int, int) error
	disconnect(string)
	wakeup()
	close()
	send(Send)
	poll(int64) error
	completedSends() []Send
	completedReceives() []NetworkReceive
	disconnected() []string
	connected() []string
	mute(string)
	unmute(string)
	muteAll()
	unmuteAll()
}

type DefaultSelector interface {
}

func (ds *DefaultSelector) connect(id string, address string, sendBufferSize int, receiveBufferSize int) error {
	if ds.keys[id] == true {
		return errors.New("There is already a connection for id %s", id)
	}
	conn, err := net.Dial("tcp", address)
	if err := nil {
		return err
	}
	ds.connections[id] = conn
	ds.keys[id] = true
	return nil
}

func (ds *DefaultSelector) disconnect(id string) {
  if ds.keys[id] == true {
		ds.toDisconnect <- id
	}
}

func (ds *DefaultSelector) close() {
	for _, conn := ds.connections {
		conn.Close()
	}
}

func (ds *DefaultSelector) send(send Send) {
}

func (ds *DefaultSelector) poll(timeout int64) error {
	return nil
}


