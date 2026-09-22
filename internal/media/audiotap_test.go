package media

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAudioTap_PublishDeliversToSubscriber(t *testing.T) {
	tap := NewAudioTap()

	ch, unsubscribe := tap.Subscribe()
	defer unsubscribe()

	tap.Publish([]byte{1, 2, 3, 4})

	select {
	case got := <-ch:
		assert.Equal(t, []byte{1, 2, 3, 4}, got)
	case <-time.After(time.Second):
		t.Fatal("subscriber never received the published frame")
	}
}

func TestAudioTap_PublishCopiesFrame(t *testing.T) {
	tap := NewAudioTap()

	ch, unsubscribe := tap.Subscribe()
	defer unsubscribe()

	frame := []byte{9, 9, 9, 9}
	tap.Publish(frame)
	frame[0] = 0xff // mutate the caller's buffer after publishing

	got := <-ch
	assert.Equal(t, []byte{9, 9, 9, 9}, got, "subscriber must see the frame as it was at publish time, not a shared, later-mutated buffer")
}

func TestAudioTap_PublishWithNoSubscribersIsNoop(t *testing.T) {
	tap := NewAudioTap()

	assert.NotPanics(t, func() { tap.Publish([]byte{1, 2, 3}) })
}

func TestAudioTap_SlowSubscriberDropsRatherThanBlocksPublish(t *testing.T) {
	tap := NewAudioTap()

	_, unsubscribe := tap.Subscribe() // never drained
	defer unsubscribe()

	done := make(chan struct{})

	go func() {
		for i := 0; i < 64; i++ {
			tap.Publish([]byte{byte(i)})
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Publish blocked on a full, undrained subscriber channel")
	}
}

func TestAudioTap_UnsubscribeClosesChannel(t *testing.T) {
	tap := NewAudioTap()

	ch, unsubscribe := tap.Subscribe()
	unsubscribe()

	_, open := <-ch
	assert.False(t, open, "channel must be closed after unsubscribe")
}

func TestAudioTap_UnsubscribeIsSafeToCallTwice(t *testing.T) {
	tap := NewAudioTap()

	_, unsubscribe := tap.Subscribe()
	unsubscribe()

	assert.NotPanics(t, unsubscribe)
}

func TestAudioTap_CloseClosesAllSubscribers(t *testing.T) {
	tap := NewAudioTap()

	ch1, unsub1 := tap.Subscribe()
	defer unsub1()
	ch2, unsub2 := tap.Subscribe()
	defer unsub2()

	tap.Close()

	_, open1 := <-ch1
	_, open2 := <-ch2
	assert.False(t, open1)
	assert.False(t, open2)
}

func TestAudioTap_SubscribeAfterCloseReturnsClosedChannel(t *testing.T) {
	tap := NewAudioTap()
	tap.Close()

	ch, unsubscribe := tap.Subscribe()
	defer unsubscribe()

	_, open := <-ch
	assert.False(t, open)
}

func TestAudioTap_PublishAfterCloseIsNoop(t *testing.T) {
	tap := NewAudioTap()

	ch, unsubscribe := tap.Subscribe()
	defer unsubscribe()

	tap.Close()

	assert.NotPanics(t, func() { tap.Publish([]byte{1, 2, 3}) })

	_, open := <-ch
	assert.False(t, open, "closed by Close, not left open by the no-op Publish")
}

func TestAudioTap_CloseIsIdempotent(t *testing.T) {
	tap := NewAudioTap()

	_, unsubscribe := tap.Subscribe()
	defer unsubscribe()

	tap.Close()
	assert.NotPanics(t, tap.Close)
}

func TestAudioTap_UnsubscribeAfterCloseIsSafe(t *testing.T) {
	tap := NewAudioTap()

	_, unsubscribe := tap.Subscribe()

	tap.Close()
	assert.NotPanics(t, unsubscribe)
}

func TestAudioTapRegistry_RegisterLookupUnregister(t *testing.T) {
	const callID = "test-call-registry-1"

	require.Nil(t, LookupAudioTap(callID), "must be nil before registration")

	tap := RegisterAudioTap(callID)
	require.NotNil(t, tap)

	assert.Same(t, tap, LookupAudioTap(callID))

	ch, unsubscribe := tap.Subscribe()
	defer unsubscribe()

	UnregisterAudioTap(callID)
	assert.Nil(t, LookupAudioTap(callID))

	_, open := <-ch
	assert.False(t, open, "UnregisterAudioTap must close the tap so a live debug stream ends with the call")
}

func TestAudioTapRegistry_UnregisterUnknownCallIDIsNoop(t *testing.T) {
	assert.NotPanics(t, func() { UnregisterAudioTap("never-registered") })
}
