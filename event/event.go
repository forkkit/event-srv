package event

import (
	"sync"
	"time"

	"github.com/micro/event-srv/db"
	"github.com/micro/go-micro/errors"
	"github.com/micro/go-platform/event/proto"
	"golang.org/x/net/context"

	"github.com/pborman/uuid"
)

var (
	mtx        sync.RWMutex
	subs       = make(map[string][]*sub)
	all        = "__*__"
	bufferSize = 1000
)

type sub struct {
	id string
	ch chan *event.Record
}

func Process(ctx context.Context, rec *event.Record) error {
	if rec == nil {
		return errors.BadRequest("go.micro.srv.event.Process", "invalid record")
	}

	if len(rec.Id) == 0 {
		return errors.BadRequest("go.micro.srv.event.Process", "invalid id")
	}

	if len(rec.Type) == 0 {
		return errors.BadRequest("go.micro.srv.event.Process", "invalid type")
	}

	if rec.Timestamp == 0 {
		return errors.BadRequest("go.micro.srv.event.Process", "invalid timestamp")
	}

	if err := db.Create(rec); err != nil {
		return errors.InternalServerError("go.micro.srv.event.Process", err.Error())
	}

	return nil
}

func Stream(ctx context.Context, rec *event.Record) error {
	if rec == nil {
		return errors.BadRequest("go.micro.srv.event.Process", "invalid record")
	}

	if len(rec.Id) == 0 {
		return errors.BadRequest("go.micro.srv.event.Process", "invalid id")
	}

	if len(rec.Type) == 0 {
		return errors.BadRequest("go.micro.srv.event.Process", "invalid type")
	}

	if rec.Timestamp == 0 {
		return errors.BadRequest("go.micro.srv.event.Process", "invalid timestamp")
	}

	mtx.RLock()
	defer mtx.RUnlock()

	gsubscribers := subs[all]
	subscribers := subs[rec.Type]

	// send to subscribers in a go channel
	go func() {
		// send to global subscribers
		for _, sub := range gsubscribers {
			select {
			case sub.ch <- rec:
			case <-time.After(time.Millisecond * 100):
			}
		}

		// send to type subscribers
		for _, sub := range subscribers {
			select {
			case sub.ch <- rec:
			case <-time.After(time.Millisecond * 100):
			}
		}
	}()

	return nil
}

func Sub(types []string) (chan *event.Record, chan bool) {
	mtx.Lock()
	defer mtx.Unlock()

	exit := make(chan bool)

	bus := &sub{
		id: uuid.NewUUID().String(),
		ch: make(chan *event.Record, bufferSize),
	}

	// global subscribe
	if len(types) == 0 {
		subs[all] = append(subs[all], bus)
	}

	// type subscribe
	for _, typ := range types {
		subs[typ] = append(subs[typ], bus)
	}

	go func() {
		<-exit
		// we've been unsubscribed
		mtx.Lock()

		// remove the global subscriber
		if len(types) == 0 {
			var subscribers []*sub

			for _, s := range subs[all] {
				if s.id == bus.id {
					continue
				}
				subscribers = append(subscribers, s)
			}

			subs[all] = subscribers
		}

		// remove subscribers for type
		for _, typ := range types {
			var subscribers []*sub

			for _, s := range subs[typ] {
				if s.id == bus.id {
					continue
				}
				subscribers = append(subscribers, s)
			}

			subs[typ] = subscribers
		}

		mtx.Unlock()
	}()

	return bus.ch, exit
}
