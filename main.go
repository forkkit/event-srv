package main

import (
	"log"

	"github.com/micro/cli"
	"github.com/micro/go-micro"
	"github.com/micro/go-micro/server"
	"github.com/microhq/event-srv/db"
	"github.com/microhq/event-srv/db/mysql"
	"github.com/microhq/event-srv/event"
	"github.com/microhq/event-srv/handler"
	proto "github.com/microhq/event-srv/proto/event"
)

var (
	Url = "root@tcp(127.0.0.1:3306)/event"
)

func main() {
	service := micro.NewService(
		micro.Name("go.micro.srv.event"),
		micro.Flags(
			cli.StringFlag{
				Name:   "database_url",
				EnvVar: "DATABASE_URL",
				Usage:  "The database URL e.g root@tcp(127.0.0.1:3306)/event",
			},
		),

		micro.Action(func(c *cli.Context) {
			if len(c.String("database_url")) > 0 {
				mysql.Url = c.String("database_url")
			}
		}),
	)

	service.Init()

	proto.RegisterEventHandler(service.Server(), new(handler.Event))

	service.Server().Subscribe(
		service.Server().NewSubscriber(
			"micro.event.record",
			event.Process,
			server.SubscriberQueue("event-srv"),
		),
	)

	// For watchers
	service.Server().Subscribe(
		service.Server().NewSubscriber(
			"micro.event.record",
			event.Stream,
		),
	)

	if err := db.Init(); err != nil {
		log.Fatal(err)
	}

	if err := service.Run(); err != nil {
		log.Fatal(err)
	}
}
