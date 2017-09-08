package main

import (
	"oss/log"
	"oss/conf"
)

func main() {
	logger := osslog.SetLogPath("./log")
	defer osslog.CloseLog(logger)

	conf.ConfigParser("D:/Studio/GoProject/ceph/OSS/src/oss/.s3cfg")
}
