# Event Server

Event server is used to store a time series set of events for the platform. Services can subscribe for event notifications

## Getting started

1. Install Consul

	Consul is the default registry/discovery for go-micro apps. It's however pluggable.
	[https://www.consul.io/intro/getting-started/install.html](https://www.consul.io/intro/getting-started/install.html)

2. Run Consul
	```
	$ consul agent -server -bootstrap-expect 1 -data-dir /tmp/consul
	```

3. Start a mysql database

4. Download and start the service

	```shell
	go get github.com/microhq/event-srv
	event-srv --database_url="root:root@tcp(192.168.99.100:3306)/event"
	```

	OR as a docker container

	```shell
	docker run microhq/event-srv --database_url="root:root@tcp(192.168.99.100:3306)/event" --registry_address=YOUR_REGISTRY_ADDRESS
	```

## The API
Event server implements the following RPC Methods

Event
- Create
- Read
- Search
- Stream

Supported but should not really be used
- Update
- Delete

