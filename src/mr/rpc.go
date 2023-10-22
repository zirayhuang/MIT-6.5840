package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
)
import "strconv"

type AssignTaskRequest struct {
}

type AssignTaskReply struct {
	Task Task
}

type NotifyTaskDoneRequest struct {
	Id       int
	TaskType TaskType
}

type NotifyTaskDoneReply struct {
}

type StatusRequest struct {
}

type StatusReply struct {
	Status Phase
}

type GetReduceNRequest struct {
}

type GetReduceNReply struct {
	ReduceN int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
