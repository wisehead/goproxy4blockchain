package utils

import (
	"log"
	"os"
)

//LogErr is to print error message.
func LogErr(v ...interface{}) {

	logfile := os.Stdout
	log.Println(v...)
	logger := log.New(logfile, "\r\n", log.Llongfile|log.Ldate|log.Ltime)
	logger.SetPrefix("[Error]")
	logger.Println(v...)
	defer logfile.Close()
}

//Log is to print general message.
func Log(v ...interface{}) {

	logfile := os.Stdout
	log.Println(v...)
	logger := log.New(logfile, "\r\n", log.Ldate|log.Ltime)
	logger.SetPrefix("[Info]")
	logger.Println(v...)
	defer logfile.Close()
}

//LogDebug is to print debug message.
func LogDebug(v ...interface{}) {
	logfile := os.Stdout
	log.Println(v...)
	logger := log.New(logfile, "\r\n", log.Ldate|log.Ltime)
	logger.SetPrefix("[Debug]")
	logger.Println(v...)
	defer logfile.Close()
}

//CheckError is to check whether there is an error, if so print it out.
func CheckError(err error) {
	if err != nil {
		LogErr(os.Stderr, "Fatal error: %s", err.Error())
	}
}
