package mysql

var (
	eventSchema = `
CREATE TABLE IF NOT EXISTS events (
id varchar(36) primary key,
type varchar(255),
origin varchar(255),
timestamp integer,
root_id varchar(36),
metadata text,
data text,
index(root_id),
index(type),
index(timestamp));`
)
