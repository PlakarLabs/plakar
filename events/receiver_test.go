package events

import "testing"

func TestReceiver(t *testing.T) {
	items := []int{1, 2, 3, 4, 5}

	receiver := New()

	listener1 := receiver.Listen()
	listener2 := receiver.Listen()

	go func() {
		for i := range items {
			receiver.Send(items[i])
		}
		receiver.Close()
	}()

	got1 := []int{}
	got2 := []int{}
	for {
		select {
		case x, ok := <-listener1:
			if !ok {
				listener1 = nil
			} else {
				got1 = append(got1, x.(int))
			}
		case x, ok := <-listener2:
			if !ok {
				listener2 = nil
			} else {
				got2 = append(got2, x.(int))
			}
		}

		if listener1 == nil && listener2 == nil {
			break
		}
	}

	if len(got1) != len(got2) {
		t.Fatalf("different number of events received, %d vs %d (want %d)",
			len(got1), len(got2), len(items))
	}

	if len(got1) != len(items) {
		t.Fatalf("unexpected number of events received: got %d, want %d",
			len(got1), len(items))
	}

	for i := range items {
		if got1[i] != got2[i] {
			t.Errorf("unexpected event #%d: %d vs %d (want %d)",
				i, got1[i], got2[i], items[i])
		}

		if got1[i] != items[i] {
			t.Errorf("unexpected event #%d: got %d, want %d",
				i, got1[i], items[i])
		}
	}
}
