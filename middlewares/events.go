package middlewares

import "sync"

type EventType string

const (
	UserRegistrationFailed EventType = "user.registration.failed"
)

type Event struct {
	Type EventType
	Data any
}

type UserRegistrationFailedPayload struct {
	Email   string
	UserUID int64
	Reason  error
}

type Handler func(any)

type EventBus struct {
	mu       sync.RWMutex
	handlers map[EventType][]Handler
}

func NewEventBus() *EventBus {
	return &EventBus{
		handlers: make(map[EventType][]Handler),
	}
}

func (b *EventBus) Subscribe(eventType EventType, handler Handler) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.handlers[eventType] = append(
		b.handlers[eventType],
		handler,
	)
}

func (b *EventBus) Publish(eventType EventType, payload any) {
	b.mu.RLock()
	handlers := b.handlers[eventType]
	b.mu.RUnlock()

	for _, handler := range handlers {
		go handler(payload) // async
	}
}
