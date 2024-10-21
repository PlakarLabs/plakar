package events

type EventsReceiver struct {
	listeners []chan interface{}
}

func New() *EventsReceiver {
	return &EventsReceiver{
		listeners: make([]chan interface{}, 0),
	}
}

func (er *EventsReceiver) Listen() <-chan interface{} {
	ch := make(chan interface{})
	er.listeners = append(er.listeners, ch)
	return ch
}

func (er *EventsReceiver) Send(event interface{}) {
	for _, ch := range er.listeners {
		ch <- event
	}
}

func (er *EventsReceiver) Close() {
	for _, ch := range er.listeners {
		close(ch)
	}
	er.listeners = make([]chan interface{}, 0)
}
