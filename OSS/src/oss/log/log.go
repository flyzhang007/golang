package osslog

import (
	"log"
	"os"
	"runtime/debug"
	"strconv"
	"time"
)

var logger = log.New(nil, "", log.LstdFlags)

func SetLogPath(path string) *os.File {
	os.MkdirAll(path, os.ModePerm)
	filename := path + "/" + "oss" + "_" + strconv.Itoa(os.Getpid()) + "_" + time.Now().Format("20060102150405") + ".log"
	logfile, err := os.Create(filename)
	if err != nil {
		CloseLog(logfile)
		log.Fatalln("create log file error.")
	}

	logger.SetOutput(logfile)

	return logfile
}

func CloseLog(logfile *os.File) {
	if logfile != nil {
		logfile.Close()
	}
}

func Infof(format string, v ...interface{}) {
	logger.SetPrefix("[INFO]")
	logger.Printf(format, v...)
}

func Debugf(format string, v ...interface{}) {
	logger.SetPrefix("[DEBUG]")
	logger.Printf(format, v...)
}

func Warnf(format string, v ...interface{}) {
	logger.SetPrefix("[WARNNIG]")
	logger.Printf(format, v...)
}

func Errorf(format string, v ...interface{}) {
	logger.SetPrefix("[ERROR]")
	logger.Printf(format, v...)
}

func Fatalf(format string, v ...interface{}) {
	logger.SetPrefix("[FATAL]")
	logger.Fatalf(format, v...)
}

func Tracef() {
	logger.SetPrefix("[TRACE]")
	logger.Println(debug.Stack())
}
