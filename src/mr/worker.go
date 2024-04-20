package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func writeKeyValues(file *os.File, kva []KeyValue) bool {
	encoder := json.NewEncoder(file)
	for _, kv := range kva {
		if err := encoder.Encode(&kv); err != nil {
			return false
		}
	}
	return true
}

func readKeyValues(file *os.File) []KeyValue {
	kva := []KeyValue{}
	decoder := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := decoder.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}
	return kva
}

func doMapTask(mapf func(string, string) []KeyValue, filename string, taskId int, nReduce int) bool {
	// open file
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		return false
	}
	content, err := io.ReadAll(file)
	file.Close()
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		return false
	}

	// perform map task
	kva := mapf(filename, string(content))

	// partition intermediate results
	buckets := make(map[int][]KeyValue)
	for _, kv := range kva {
		bucketId := ihash(kv.Key) % nReduce
		kvPart, exist := buckets[bucketId]
		if !exist {
			kvPart = []KeyValue{}
		}
		kvPart = append(kvPart, kv)
		buckets[bucketId] = kvPart
	}

	// write intermediate results to files
	for bucketId := 0; bucketId < nReduce; bucketId++ {
		kvPart, exist := buckets[bucketId]
		if !exist {
			kvPart = []KeyValue{}
		}

		outFileName := fmt.Sprintf("mr-%d-%d", taskId, bucketId)
		outFile, err := os.Create(outFileName)
		if err != nil {
			return false
		}
		if !writeKeyValues(outFile, kvPart) {
			return false
		}
		outFile.Close()
	}
	return true
}

func doReduceTask(reducef func(string, []string) string, taskId int, nMap int) bool {
	kva := []KeyValue{}
	// load intermediate key-values
	for partition := 0; partition < nMap; partition++ {
		filename := fmt.Sprintf("mr-%d-%d", partition, taskId)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
			return false
		}

		kva = append(kva, readKeyValues(file)...)
		file.Close()
	}

	sort.Sort(ByKey(kva))

	outFileName := fmt.Sprintf("mr-out-%d", taskId)
	outFile, err := os.Create(outFileName)
	if err != nil {
		return false
	}

	// perform reduce and generate result
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(outFile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	outFile.Close()
	return true
}

func retry(f func() bool, nRetries int, waitDuration time.Duration) bool {
	for i := 0; i < nRetries; i++ {
		if ok := f(); ok {
			return true
		}
	}
	return false
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	nRetries := 3
	waitDuration := 100 * time.Millisecond

	for {
		req := TaskRequest{}
		resp := TaskResponse{}

		if ok := retry(func() bool {
			return call("Coordinator.HandleTaskRequest", &req, &resp)
		}, nRetries, waitDuration); !ok {
			fmt.Println("failed to reach coordinator, exiting")
			return
		}

		taskId := resp.TaskId

		switch resp.Action {
		case "done":
			return
		case "wait":
			time.Sleep(waitDuration)
			continue
		case "map":
			doMapTask(mapf, resp.FileName, taskId, resp.NTasks)
		case "reduce":
			doReduceTask(reducef, taskId, resp.NTasks)
		}

		fmt.Printf("%v task %d done\n", resp.Action, taskId)

		// signal coordinator the task is done
		ack := TaskRequest{TaskId: taskId}
		retry(func() bool {
			return call("Coordinator.FinishTask", &ack, &resp)
		}, nRetries, waitDuration)
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
