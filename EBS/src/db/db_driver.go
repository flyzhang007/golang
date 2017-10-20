package db

import "database/sql"
import _"github.com/go-sql-driver/mysql"

const (
	VolumesTab         = "volumes"
	ClientVolumesTab   = "client_volumes"
	TargetVolumesTab   = "target_volumes"
)

type dbHandle struct {
	dsn string
	handle *sql.DB
}

var handler = &dbHandle{
	dsn: "",
	handle: nil,
}

func connectDB() (*sql.DB, error) {
	handle, err := sql.Open("mysql", handler.dsn)
	if err != nil {
		return nil, err
	}

	if err := handle.Ping(); err != nil {
		handle.Close()
		return nil, err
	}

	return handle, nil
}

func Init() error {
	handler.dsn = "root:123456@tcp(172.7.102.214:3306)/ebs"
	handle, err := connectDB()
	if err != nil {
		return err
	}
	handler.handle = handle
	return nil
}

func Destroy() {
	handler.handle.Close()
}

func GetDBHandler() *sql.DB {
	return handler.handle
}
