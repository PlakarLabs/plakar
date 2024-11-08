package events

type Receiver struct {
	listeners []chan interface{}
}

func New() *Receiver {
	return &Receiver{
		listeners: make([]chan interface{}, 0),
	}
}

func (er *Receiver) Listen() <-chan interface{} {
	ch := make(chan interface{})
	er.listeners = append(er.listeners, ch)
	return ch
}

func (er *Receiver) Send(event interface{}) {
	for _, ch := range er.listeners {
		ch <- event
	}
}

func (er *Receiver) Close() {
	for _, ch := range er.listeners {
		close(ch)
	}
	er.listeners = make([]chan interface{}, 0)
}
