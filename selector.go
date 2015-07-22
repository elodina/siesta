package siesta

import "net"

type Selector interface {
	connect(string, net.Addr, int, int) error
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
