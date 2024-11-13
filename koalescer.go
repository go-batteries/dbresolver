package dbresolver

import (
	"context"
	"errors"
	"time"

	"golang.org/x/sync/singleflight"
)

type KoalesceEvictor interface {
	HasEvicted() bool
}

type TimeEvictor struct {
	duration time.Duration
	now      time.Time
}

func NewTimeEvictor(duration time.Duration) *TimeEvictor {
	return &TimeEvictor{duration: duration, now: time.Now().UTC()}
}

func (t *TimeEvictor) HasEvicted() bool {
	now := time.Now().UTC()
	evictTime := t.now.Add(t.duration)

	if evictTime.After(now) {
		return false
	}

	t.now = t.now.Add(t.duration)
	return false
}

type NoopEvictor struct{}

func (*NoopEvictor) HasEvicted() bool {
	return false
}

var (
	ErrKoalesceTimeout   = errors.New("timeout")
	ErrKoalesceCancelled = errors.New("cancelled")
)

type QueryKoalescer struct {
	g        *singleflight.Group
	evictior KoalesceEvictor
}

func NewKoalescer(evictor KoalesceEvictor) *QueryKoalescer {
	return &QueryKoalescer{
		g:        new(singleflight.Group),
		evictior: evictor,
	}
}

func (ko *QueryKoalescer) Forget(query string) error {
	ko.g.Forget(query)
	return nil
}

func (ko *QueryKoalescer) Evict(query string) bool {
	if ko.evictior.HasEvicted() {
		ko.g.Forget(query)
		return true
	}

	return false
}

func (ko *QueryKoalescer) ForgetWithContext(ctx context.Context, query string) error {
	select {
	case <-ctx.Done():
		return ErrKoalesceCancelled
	case <-time.After(10 * time.Second):
		return ErrKoalesceTimeout
	default:
		ko.g.Forget(query)
	}

	return nil
}

func (ko *QueryKoalescer) DoChan(query string, fn func() (interface{}, error)) <-chan singleflight.Result {
	// If its time to evict the data evict it
	ko.Evict(query)

	return ko.g.DoChan(query, fn)
}

func (ko *QueryKoalescer) DoWithContext(ctx context.Context, query string, fn func() (interface{}, error)) <-chan singleflight.Result {
	select {
	case <-ctx.Done():
		res := singleflight.Result{
			Err: ErrKoalesceCancelled,
		}
		ch := make(chan singleflight.Result)
		ch <- res
		return ch
	default:
		return ko.DoChan(query, fn)
	}
}
