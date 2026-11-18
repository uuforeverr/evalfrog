package projection

import (
	"context"
	"sync"
)

// LocalNotifier is a process-local, lossy Run update fan-out. It deliberately
// carries no Runtime state: subscribers always re-read the authoritative Run
// view. Cross-instance and missed wake-ups are recovered by the SSE version
// poller.
type LocalNotifier struct {
	mu          sync.Mutex
	nextID      uint64
	subscribers map[string]map[uint64]chan string
}

func NewLocalNotifier() *LocalNotifier {
	return &LocalNotifier{subscribers: make(map[string]map[uint64]chan string)}
}

func (notifier *LocalNotifier) PublishRunUpdate(_ context.Context, runID string) {
	if notifier == nil || runID == "" {
		return
	}
	notifier.mu.Lock()
	defer notifier.mu.Unlock()
	for _, channel := range notifier.subscribers[runID] {
		select {
		case channel <- runID:
		default:
			// Coalesce duplicate wake-ups. The subscriber reloads authority.
		}
	}
}

func (notifier *LocalNotifier) SubscribeRunUpdates(ctx context.Context, runID string) (<-chan string, func()) {
	channel := make(chan string, 1)
	if notifier == nil || runID == "" {
		close(channel)
		return channel, func() {}
	}
	notifier.mu.Lock()
	notifier.nextID++
	id := notifier.nextID
	if notifier.subscribers[runID] == nil {
		notifier.subscribers[runID] = make(map[uint64]chan string)
	}
	notifier.subscribers[runID][id] = channel
	notifier.mu.Unlock()

	var once sync.Once
	unsubscribe := func() {
		once.Do(func() {
			notifier.mu.Lock()
			defer notifier.mu.Unlock()
			if subscribers := notifier.subscribers[runID]; subscribers != nil {
				if current, exists := subscribers[id]; exists {
					delete(subscribers, id)
					close(current)
				}
				if len(subscribers) == 0 {
					delete(notifier.subscribers, runID)
				}
			}
		})
	}
	go func() {
		<-ctx.Done()
		unsubscribe()
	}()
	return channel, unsubscribe
}
