package projection

import (
	"context"
	"testing"
	"time"
)

func TestLocalNotifierFansOutAndCoalescesWithoutSubscribers(t *testing.T) {
	notifier := NewLocalNotifier()
	notifier.PublishRunUpdate(context.Background(), "unobserved")

	ctx, cancel := context.WithCancel(context.Background())
	updates, unsubscribe := notifier.SubscribeRunUpdates(ctx, "run-1")
	defer unsubscribe()
	notifier.PublishRunUpdate(context.Background(), "run-1")
	select {
	case runID := <-updates:
		if runID != "run-1" {
			t.Fatalf("run id=%q", runID)
		}
	case <-time.After(time.Second):
		t.Fatal("local wake-up was not delivered")
	}
	cancel()
	select {
	case _, open := <-updates:
		if open {
			t.Fatal("subscription remained open after cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("subscription was not closed after cancellation")
	}
}
