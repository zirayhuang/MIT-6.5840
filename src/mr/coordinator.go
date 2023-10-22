package mr

import (
	"log"
	"path/filepath"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	mapTasks    []Task
	reduceTasks []Task
	taskCounter int
	mu          sync.Mutex
	wg          sync.WaitGroup
	reduceN     int
	phase       Phase
}

type TaskType int
type Phase int
type TaskStatus int

const (
	MapTask TaskType = iota
	ReduceTask
	Exit
	NoTask
)

const (
	Map Phase = iota
	Reduce
	Done
)

const (
	Unassigned TaskStatus = iota
	Executing
	Finished
)

type Task struct {
	TaskId     int
	Filename   string
	TaskType   TaskType
	TaskStatus TaskStatus
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) AssignWork(args *AssignTaskRequest, reply *AssignTaskReply) error {
	if c.phase == Done {
		reply.Task.TaskType = Exit
		return nil
	}
	c.mu.Lock()
	task, result := c.findUnassignedTask()

	if !result {
		reply.Task.TaskType = NoTask
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()
	reply.Task = task
	if task.TaskType == ReduceTask {
	}
	go c.checkCompleteness(task.TaskId, task.TaskType)

	return nil
}

func (c *Coordinator) NotifyTaskDone(args *NotifyTaskDoneRequest, reply *NotifyTaskDoneReply) error {
	c.mu.Lock()
	if args.TaskType == MapTask {
		if c.mapTasks[args.Id].TaskStatus == Executing {
			c.mapTasks[args.Id].TaskStatus = Finished
			c.wg.Done()
		}
	} else if args.TaskType == ReduceTask {
		if c.reduceTasks[args.Id].TaskStatus == Executing {
			c.reduceTasks[args.Id].TaskStatus = Finished
			c.wg.Done()
		}

	}
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) QueryStatus(args *StatusRequest, reply *StatusReply) error {
	reply.Status = c.phase
	return nil
}

func (c *Coordinator) GetReduceN(args *GetReduceNRequest, reply *GetReduceNReply) error {
	reply.ReduceN = c.reduceN
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
	return c.phase == Done
}

func (c *Coordinator) Process() {
	// Map Phase
	c.wg.Wait()
	// Reduce Phase
	c.mu.Lock()
	c.phase = Reduce
	c.wg = sync.WaitGroup{}
	c.wg.Add(c.reduceN)
	c.mu.Unlock()
	c.wg.Wait()
	c.phase = Done
}

func (c *Coordinator) findUnassignedTask() (Task, bool) {
	if c.phase == Map {
		for i := range c.mapTasks {
			task := &c.mapTasks[i]
			if task.TaskStatus == Unassigned {
				task.TaskStatus = Executing
				return *task, true
			}
		}
	} else if c.phase == Reduce {
		for i := range c.reduceTasks {
			task := &c.reduceTasks[i]
			if task.TaskStatus == Unassigned {
				task.TaskStatus = Executing
				return *task, true
			}
		}
	}
	var emptyTask Task
	return emptyTask, false
}

func (c *Coordinator) checkCompleteness(i int, taskType TaskType) {
	time.Sleep(10 * time.Second)
	c.mu.Lock()
	defer c.mu.Unlock()

	if taskType == MapTask {
		if c.mapTasks[i].TaskStatus == Executing {
			c.mapTasks[i].TaskStatus = Unassigned
		}
	} else if taskType == ReduceTask {
		if c.reduceTasks[i].TaskStatus == Executing {
			c.reduceTasks[i].TaskStatus = Unassigned
		}
	}
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{reduceN: nReduce, phase: Map}
	c.mapTasks = make([]Task, len(files))
	c.reduceTasks = make([]Task, nReduce)
	// prepare map tasks
	for i, file := range files {
		c.mapTasks[i] = Task{TaskId: i, Filename: file, TaskStatus: Unassigned, TaskType: MapTask}
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = Task{TaskId: i, TaskStatus: Unassigned, TaskType: ReduceTask}
	}
	c.wg.Add(len(files))
	// clean up and create temp directory
	outFiles, _ := filepath.Glob("mr-out*")
	for _, f := range outFiles {
		if err := os.Remove(f); err != nil {
			log.Fatalf("Cannot remove file %v\n", f)
		}
	}
	err := os.RemoveAll(TempDir)
	if err != nil {
		log.Fatalf("Cannot remove temp directory %v\n", TempDir)
	}
	err = os.Mkdir(TempDir, 0755)
	if err != nil {
		log.Fatalf("Cannot create temp directory %v\n", TempDir)
	}

	go c.Process()
	c.server()
	return &c
}
