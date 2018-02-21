package log

import (
	"log"
	"os"
	"sync"
)

type LogFiles struct {
	Info  string
	Error string
	Trace string
	Stats string
}

//Singleton ...
type Singleton struct {
	InfoLog  *log.Logger
	StatsLog *log.Logger
	TraceLog *log.Logger
	ErrorLog *log.Logger
}

var instance *Singleton
var once sync.Once

//GetInstance ...
func GetInstance() *Singleton {
	once.Do(func() {
		instance = &Singleton{}
		instance.InfoLog = log.New(os.Stdout, "INFO ", log.Flags())
		instance.StatsLog = log.New(os.Stdout, "STATS ", log.Flags())
		instance.TraceLog = log.New(os.Stdout, "TRACE ", log.Flags())
		instance.ErrorLog = log.New(os.Stderr, "ERROR ", log.Flags())

	})
	return instance
}
