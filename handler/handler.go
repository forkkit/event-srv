package handler

import (
	"time"

	"github.com/micro/event-srv/db"
	ev "github.com/micro/event-srv/event"
	event "github.com/micro/event-srv/proto/event"
	"github.com/micro/go-micro/errors"
	"golang.org/x/net/context"

	"github.com/pborman/uuid"
)

type Event struct{}

func (e *Event) Create(ctx context.Context, req *event.CreateRequest, rsp *event.CreateResponse) error {
	if req.Record == nil {
		return errors.BadRequest("go.micro.srv.event.Create", "invalid record")
	}

	if req.Record.Timestamp == 0 {
		req.Record.Timestamp = time.Now().Unix()
	}

	if len(req.Record.Id) == 0 {
		req.Record.Id = uuid.NewUUID().String()
	}

	if err := db.Create(req.Record); err != nil {
		return errors.InternalServerError("go.micro.srv.event.Create", err.Error())
	}

	return nil
}

func (e *Event) Read(ctx context.Context, req *event.ReadRequest, rsp *event.ReadResponse) error {
	if len(req.Id) == 0 {
		return errors.BadRequest("go.micro.srv.event.Read", "invalid id")
	}

	rec, err := db.Read(req.Id)
	if err != nil {
		return errors.InternalServerError("go.micro.srv.event.Update", err.Error())
	}

	rsp.Record = rec

	return nil
}

func (e *Event) Update(ctx context.Context, req *event.UpdateRequest, rsp *event.UpdateResponse) error {
	if req.Record == nil {
		return errors.BadRequest("go.micro.srv.event.Update", "invalid record")
	}

	if len(req.Record.Id) == 0 {
		return errors.BadRequest("go.micro.srv.event.Update", "invalid id")
	}

	if req.Record.Timestamp == 0 {
		req.Record.Timestamp = time.Now().Unix()
	}

	if err := db.Update(req.Record); err != nil {
		return errors.InternalServerError("go.micro.srv.event.Update", err.Error())
	}

	return nil
}

func (e *Event) Delete(ctx context.Context, req *event.DeleteRequest, rsp *event.DeleteResponse) error {
	if len(req.Id) == 0 {
		return errors.BadRequest("go.micro.srv.event.Delete", "invalid id")
	}

	if err := db.Delete(req.Id); err != nil {
		return errors.InternalServerError("go.micro.srv.event.Delete", err.Error())
	}

	return nil
}

func (e *Event) Search(ctx context.Context, req *event.SearchRequest, rsp *event.SearchResponse) error {
	recs, err := db.Search(req.Id, req.Type, req.From, req.To, req.Limit, req.Offset, req.Reverse)
	if err != nil {
		return errors.InternalServerError("go.micro.srv.event.Update", err.Error())
	}

	rsp.Records = recs

	return nil
}

func (e *Event) Stream(ctx context.Context, req *event.StreamRequest, stream event.Event_StreamStream) error {
	sub, exit := ev.Sub(req.Types)
	defer close(exit)

	for {
		record := <-sub

		if err := stream.Send(&event.StreamResponse{
			Record: record,
		}); err != nil {
			return errors.InternalServerError("go.micro.srv.event.Stream", err.Error())
		}
	}

	return nil
}
