package raft

import (
	"fmt"
	"log"
	"net/http"
	"net/http/pprof"
	"os"
	"strconv"
	"sync"
	"time"
)

// Debugging
const Debug = false

var debugStart time.Time
var debugVerbosity int
var mutex sync.Mutex
var initialized bool = false

type logTopic string

const (
	dClient  logTopic = "CLNT"
	dCommit  logTopic = "CMIT"
	dDrop    logTopic = "DROP"
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dLeader  logTopic = "LEAD"
	dLog1    logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTerm    logTopic = "TERM"
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR"
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE"
	dWarn    logTopic = "WARN"
)

// Retrieve the verbosity level from an environment variable
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

const (
	pprofAddr string = ":7890"
)

func startHTTPDebugger() {
	pprofHandler := http.NewServeMux()
	pprofHandler.Handle("/debug/pprof/", http.HandlerFunc(pprof.Index))
	server := &http.Server{Addr: pprofAddr, Handler: pprofHandler}
	go server.ListenAndServe()
}

func InitDebug() {
	mutex.Lock()
	defer mutex.Unlock()

	if !initialized {
		debugVerbosity = getVerbosity()
		debugStart = time.Now()

		log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))

		startHTTPDebugger()

		initialized = true
	}
}

func DPrint(topic logTopic, format string, a ...interface{}) {
	if debugVerbosity > 0 {
		time := time.Since(debugStart).Milliseconds()
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}
