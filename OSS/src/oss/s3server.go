package main

import (
	"net/http"
	"oss/conf"
	"oss/log"
	"oss/router"
)

func main() {
	logger := osslog.SetLogPath("./log")
	defer osslog.CloseLog(logger)

	conf.ConfigParser("D:/Studio/GoProject/ceph/OSS/src/oss/.s3cfg")

	initSvr()
}

func initSvr() {
	router.Init()
	http.ListenAndServe("0.0.0.0:5555", nil)
}
