package db

import (
	event "github.com/micro/go-platform/event/proto"
)

type DB interface {
	Init() error
	Read(id string) (*event.Record, error)
	Create(rec *event.Record) error
	Update(rec *event.Record) error
	Delete(id string) error
	Search(id, typ string, from, to, limit, offset int64, reverse bool) ([]*event.Record, error)
}

var (
	db DB
)

func Register(backend DB) {
	db = backend
}

func Init() error {
	return db.Init()
}

func Read(id string) (*event.Record, error) {
	return db.Read(id)
}

func Create(rec *event.Record) error {
	return db.Create(rec)
}

func Update(rec *event.Record) error {
	return db.Update(rec)
}

func Delete(id string) error {
	return db.Delete(id)
}

func Search(id, typ string, from, to, limit, offset int64, reverse bool) ([]*event.Record, error) {
	return db.Search(id, typ, from, to, limit, offset, reverse)
}
