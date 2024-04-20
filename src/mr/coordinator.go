package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// global state
const (
	PhaseMap = iota
	PhaseReduce
	PhaseDone
)

// task state
const (
	Unallocated = iota
	Pending
	Finished
)

type Coordinator struct {
	// Your definitions here.
	fileNames []string
	nReduce   int

	// states
	lock       sync.Mutex
	phase      int // "map", "reduce", "done"
	states     [][]int
	startTimes [][]time.Time
}

func (c *Coordinator) init() {
	c.phase = PhaseMap

	m := len(c.fileNames)
	n := c.nReduce
	lengths := []int{m, n}

	c.states = [][]int{make([]int, m), make([]int, n)}
	for i := 0; i < PhaseDone; i++ {
		for j := 0; j < lengths[i]; j++ {
			c.states[i][j] = Unallocated
		}
	}

	c.startTimes = [][]time.Time{make([]time.Time, m), make([]time.Time, n)}
}

func (c *Coordinator) checkTimeout() {
	timeout := 10 * time.Second
	period := 1 * time.Second

	check := func() bool {
		c.lock.Lock()
		defer c.lock.Unlock()

		if c.phase == PhaseDone {
			return true
		}
		now := time.Now()
		curStates := c.states[c.phase]
		for i := 0; i < len(curStates); i++ {
			if curStates[i] == Pending && now.Sub(c.startTimes[c.phase][i]) > timeout {
				curStates[i] = Unallocated
				log.Printf("phase %v task %d timeout, reset\n", c.phase, i)
			}
		}
		return false
	}

	for {
		time.Sleep(period)

		if stop := check(); stop {
			break
		}
	}
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) HandleTaskRequest(req *TaskRequest, resp *TaskResponse) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.phase == PhaseDone {
		resp.Action = "done"
		return nil
	}

	curStates := c.states[c.phase]
	nextIndex := -1
	for i := 0; i < len(curStates); i++ {
		if curStates[i] == Unallocated {
			nextIndex = i
			break
		}
	}
	if nextIndex < 0 {
		resp.Action = "wait"
		return nil
	}

	resp.TaskId = nextIndex
	curStates[nextIndex] = Pending
	if c.phase == PhaseMap {
		resp.Action = "map"
		resp.FileName = c.fileNames[nextIndex]
		resp.NTasks = c.nReduce
	} else if c.phase == PhaseReduce {
		resp.Action = "reduce"
		resp.NTasks = len(c.fileNames)
	}
	c.startTimes[c.phase][nextIndex] = time.Now()
	log.Printf("phase %v allocate task %d\n", c.phase, nextIndex)
	return nil
}

func (c *Coordinator) FinishTask(req *TaskRequest, resp *TaskResponse) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.phase == PhaseDone {
		return nil
	}

	// TODO: bounary check
	curStates := c.states[c.phase]
	curStates[req.TaskId] = Finished
	log.Printf("global state %d finish %d\n", c.phase, req.TaskId)

	allFinished := true
	for i := 0; i < len(curStates); i++ {
		if curStates[i] != Finished {
			allFinished = false
			break
		}
	}
	if allFinished {
		c.phase++
		log.Printf("current global state %v\n", c.phase)
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.phase == PhaseDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{fileNames: files, nReduce: nReduce}

	// Your code here.

	c.init()
	go c.checkTimeout()

	c.server()
	return &c
}
