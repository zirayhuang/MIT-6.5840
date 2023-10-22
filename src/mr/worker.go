package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

type ByKey []KeyValue

const TempDir = "tmp"

var NReduce int

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	ok, n := getNReduce()
	if ok == false {
		fmt.Println("Failed to get reduce number, exiting")
		return
	}

	NReduce = n

	for {
		success, task := getTask()
		if success == false {
			fmt.Println("Failed to get task from coordinator, exiting")
			return
		}

		success = true
		if task.TaskType == NoTask {
			fmt.Println("No available task, waiting")
		} else if task.TaskType == MapTask {
			doMapTask(mapf, task)
			success = notifyTaskDone(task.TaskId, MapTask)
		} else if task.TaskType == ReduceTask {
			doReduceTask(reducef, task)
			success = notifyTaskDone(task.TaskId, ReduceTask)
		} else if task.TaskType == Exit {
			fmt.Println("All tasks are done , exiting.")
			return
		}
		if success == false {
			fmt.Println("Failed to notify coordinator, exiting")
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func doMapTask(mapf func(string, string) []KeyValue, task Task) {
	content, err := os.ReadFile(task.Filename)
	checkError(err, "Failed to read file: %v", task.Filename)
	kva := mapf(task.Filename, string(content))
	writeMapOutput(kva, task.TaskId)
}

func doReduceTask(reducef func(string, []string) string, task Task) {
	files, err := filepath.Glob(fmt.Sprintf("%v/mr-%v-%v", TempDir, "*", task.TaskId))
	checkError(err, "Failed to get intermediate files")

	kvMap := make(map[string][]string)
	var kv KeyValue
	for _, filepath := range files {
		file, err := os.Open(filepath)
		checkError(err, "Failed to open intermediates files: %v", err)
		dec := json.NewDecoder(file)
		for dec.More() {
			err = dec.Decode(&kv)
			checkError(err, "Failed to decode intermediate files: %V", err)
			kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
		}
	}
	writeReduceOutput(reducef, kvMap, task.TaskId)
}

func writeReduceOutput(reducef func(string, []string) string, kvMap map[string][]string, reduceId int) {
	keys := make([]string, 0, len(kvMap))
	for k := range kvMap {
		keys = append(keys, k)
	}

	sort.Strings(keys)
	// Create temp file
	filePath := fmt.Sprintf("%v/mr-out-%v-%v", TempDir, reduceId, os.Getpid())
	file, err := os.Create(filePath)
	checkError(err, "failed to write reduce file: %v", err)

	// reduce and write to temp file
	for _, k := range keys {
		_, err := fmt.Fprintf(file, "%v %v\n", k, reducef(k, kvMap[k]))
		checkError(err, "failed to write reduce file: %v", err)
	}

	// atomically rename temp files to ensure no one observes partial files
	file.Close()
	newPath := fmt.Sprintf("mr-out-%v", reduceId)
	err = os.Rename(filePath, newPath)
	checkError(err, "failed to rename reduce file: %v", err)
}

func getNReduce() (bool, int) {
	args := GetReduceNRequest{}
	reply := GetReduceNReply{}
	ok := call("Coordinator.GetReduceN", &args, &reply)
	return ok, reply.ReduceN
}

// getTask Get assigned work from coordinator
func getTask() (bool, Task) {
	args := AssignTaskRequest{}
	reply := AssignTaskReply{}
	ok := call("Coordinator.AssignWork", &args, &reply)
	return ok, reply.Task
}

// notifyTaskDone Notify coordinator the task is completed
func notifyTaskDone(i int, taskType TaskType) bool {
	args := NotifyTaskDoneRequest{Id: i, TaskType: taskType}
	reply := NotifyTaskDoneReply{}
	ok := call("Coordinator.NotifyTaskDone", &args, &reply)
	return ok
}

func writeMapOutput(kva []KeyValue, taskId int) {
	// create intermediate files
	prefix := fmt.Sprintf("%v/mr-%v", TempDir, taskId)
	files := make([]*os.File, 0, NReduce)
	encoders := make([]*json.Encoder, 0, NReduce)
	buffers := make([]*bufio.Writer, 0, NReduce)

	for i := 0; i < NReduce; i++ {
		filename := fmt.Sprintf("%v-%v-%v", prefix, i, os.Getpid())
		file, err := os.Create(filename)
		checkError(err, "Failed to save intermediate file: %v", err)
		buf := bufio.NewWriter(file)
		files = append(files, file)
		buffers = append(buffers, buf)
		encoders = append(encoders, json.NewEncoder(buf))
	}

	for _, kv := range kva {
		err := encoders[ihash(kv.Key)%NReduce].Encode(&kv)
		checkError(err, "Failed to encode kv into intermediate file: %v", err)
	}

	for _, buf := range buffers {
		err := buf.Flush()
		checkError(err, "Failed to flush buffer into file: %v", err)
	}

	// atomically rename temp files to ensure no one observes partial files
	for i, file := range files {
		file.Close()
		newPath := fmt.Sprintf("%v-%v", prefix, i)
		err := os.Rename(file.Name(), newPath)
		checkError(err, "cannot rename file %v", file.Name())
	}
}

func checkError(err error, format string, a ...interface{}) {
	if err != nil {
		fmt.Printf(format, a)
		os.Exit(1)
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
