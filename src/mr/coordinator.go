package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

const (
	Idle int = iota
	InProgress
	Completed
)

type Task struct {
	TaskType string
	TaskID   int
	Filename string
	Status   int
}

type Coordinator struct {
	// Your definitions here.
	mu      sync.Mutex
	tasks   []Task
	phase   string
	nMap    int
	nReduce int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.phase == "map" {
		for i, task := range c.tasks {
			if task.Status == Idle {
				reply.WorkerID = i
				reply.TaskType = task.TaskType
				reply.Filename = task.Filename
				reply.NReduce = c.nReduce
				reply.NMap = c.nMap
				c.tasks[i].Status = InProgress
				break
			}
		}
	} else {
		for i, task := range c.tasks {
			if task.Status == Idle {
				reply.WorkerID = i
				reply.TaskType = task.TaskType
				reply.Filename = task.Filename
				reply.NReduce = c.nReduce
				reply.NMap = c.nMap
				c.tasks[i].Status = InProgress
				break
			}
		}
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
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	c.server()
	return &c
}
