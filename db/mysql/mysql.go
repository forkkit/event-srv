package mysql

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/microhq/event-srv/db"
	event "github.com/microhq/event-srv/proto/event"

	_ "github.com/go-sql-driver/mysql"
)

var (
	Url = "root@tcp(127.0.0.1:3306)/event"

	selectQ = "SELECT id, type, origin, timestamp, root_id, metadata, data from %s.%s"

	eventQ = map[string]string{
		"create": "INSERT INTO %s.%s (id, type, origin, timestamp, root_id, metadata, data) values (?, ?, ?, ?, ?, ?, ?)",
		"update": "UPDATE %s.%s set type = ?, origin = ?, timestamp = ?, root_id = ?, metadata = ?, data = ? where id = ? limit 1",
		"delete": "DELETE FROM %s.%s where id = ? limit 1",
		"read":   selectQ + " where id = ? limit 1",

		"searchAsc":               selectQ + " where timestamp >= ? and timestamp <= ? limit ? offset ?",
		"searchDesc":              selectQ + " where timestamp >= ? and timestamp <= ? order by id desc limit ? offset ?",
		"searchRootIdAsc":         selectQ + " where timestamp >= ? and timestamp <= ? and root_id = ? limit ? offset ?",
		"searchRootIdDesc":        selectQ + " where timestamp >= ? and timestamp <= ? and root_id = ? order by id desc limit ? offset ?",
		"searchTypeAsc":           selectQ + " where timestamp >= ? and timestamp <= ? and type = ? limit ? offset ?",
		"searchTypeDesc":          selectQ + " where timestamp >= ? and timestamp <= ? and type = ? order by id desc limit ? offset ?",
		"searchRootIdAndTypeAsc":  selectQ + " where timestamp >= ? and timestamp <= ? and root_id = ? and type = ? limit ? offset ?",
		"searchRootIdAndTypeDesc": selectQ + " where timestamp >= ? and timestamp <= ? and root_id = ? and type = ? order by id desc limit ? offset ?",
	}

	st = map[string]*sql.Stmt{}
)

type mysql struct {
	db *sql.DB
}

func init() {
	db.Register(new(mysql))
}

func (m *mysql) Init() error {
	var d *sql.DB
	var err error

	parts := strings.Split(Url, "/")
	if len(parts) != 2 {
		return errors.New("Invalid database url")
	}

	if len(parts[1]) == 0 {
		return errors.New("Invalid database name")
	}

	url := parts[0]
	database := parts[1]

	if d, err = sql.Open("mysql", url+"/"); err != nil {
		return err
	}
	if _, err := d.Exec("CREATE DATABASE IF NOT EXISTS " + database); err != nil {
		return err
	}
	d.Close()
	if d, err = sql.Open("mysql", Url); err != nil {
		return err
	}
	if _, err = d.Exec(eventSchema); err != nil {
		return err
	}

	for query, statement := range eventQ {
		prepared, err := d.Prepare(fmt.Sprintf(statement, database, "events"))
		if err != nil {
			return err
		}
		st[query] = prepared
	}

	m.db = d

	return nil
}

func (m *mysql) Delete(id string) error {
	_, err := st["delete"].Exec(id)
	return err
}

func (m *mysql) Update(record *event.Record) error {
	b, err := json.Marshal(record.Metadata)
	if err != nil {
		return err
	}

	_, err = st["update"].Exec(
		record.Type,
		record.Origin,
		record.Timestamp,
		record.RootId,
		string(b),
		record.Data,
		record.Id,
	)

	return err
}

func (m *mysql) Create(record *event.Record) error {
	b, err := json.Marshal(record.Metadata)
	if err != nil {
		return err
	}

	_, err = st["create"].Exec(
		record.Id,
		record.Type,
		record.Origin,
		record.Timestamp,
		record.RootId,
		string(b),
		record.Data,
	)

	return err
}

func (m *mysql) Read(id string) (*event.Record, error) {
	if len(id) == 0 {
		return nil, errors.New("Invalid event id")
	}

	r := st["read"].QueryRow(id)

	var metadata string
	record := &event.Record{}

	if err := r.Scan(
		&record.Id,
		&record.Type,
		&record.Origin,
		&record.Timestamp,
		&record.RootId,
		&metadata,
		&record.Data,
	); err != nil {
		return nil, err
	}

	if err := json.Unmarshal([]byte(metadata), &record.Metadata); err != nil {
		return nil, err
	}

	return record, nil
}

func (m *mysql) Search(id, typ string, from, to, limit, offset int64, reverse bool) ([]*event.Record, error) {
	var r *sql.Rows
	var err error

	if limit <= 0 {
		limit = 10
	}

	if offset < 0 {
		offset = 0
	}

	if from == 0 {
		from = time.Now().Add(-time.Hour).Unix()
	}

	if to == 0 {
		to = time.Now().Unix()
	}

	order := "Asc"

	if reverse {
		order = "Desc"
	}

	if len(id) > 0 && len(typ) > 0 {
		r, err = st["searchRootIdAndType"+order].Query(from, to, id, typ, limit, offset)
	} else if len(id) > 0 {
		r, err = st["searchRootId"+order].Query(from, to, id, limit, offset)
	} else if len(typ) > 0 {
		r, err = st["searchType"+order].Query(from, to, typ, limit, offset)
	} else {
		r, err = st["search"+order].Query(from, to, limit, offset)
	}

	if err != nil {
		return nil, err
	}
	defer r.Close()

	var records []*event.Record

	for r.Next() {
		var metadata string
		record := &event.Record{}

		if err := r.Scan(
			&record.Id,
			&record.Type,
			&record.Origin,
			&record.Timestamp,
			&record.RootId,
			&metadata,
			&record.Data,
		); err != nil {
			return nil, err
		}

		if err := json.Unmarshal([]byte(metadata), &record.Metadata); err != nil {
			return nil, err
		}

		records = append(records, record)
	}

	if r.Err() != nil {
		return nil, err
	}

	return records, nil
}
