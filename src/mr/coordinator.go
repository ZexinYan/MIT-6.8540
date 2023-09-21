package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type MapTask struct {
	Status    string
	MachineID int
	MapTaskID int
}

type ReduceTask struct {
	Status         string
	MachineID      int
	ReduceWorkerID int
	StartTime      time.Time
}

type Coordinator struct {
	// Your definitions here.
	mu             sync.Mutex
	mapTasks       map[string]*MapTask
	mapWorkerID    int
	mapFinished    []int
	reduceTasks    map[int]*ReduceTask
	reduceWorkerID int
	reduceFinished []int
	nReduceTask    int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) Assign(args *AssignArgs, reply *AssignReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	reply.MapTaskID = -1
	reply.ReduceTaskID = -1
	if len(c.mapFinished) != len(c.mapTasks) {
		for key, value := range c.mapTasks {
			if value.Status == "idle" {
				mapTask := c.mapTasks[key]
				mapTask.Status = "progress"
				mapTask.MachineID = args.MachineID
				reply.MapFileName = key
				reply.MapTaskID = c.mapWorkerID
				reply.NReduceTask = c.nReduceTask
				c.mapWorkerID++
				return nil
			}
		}
		for key, value := range c.mapTasks {
			if value.Status == "progress" {
				mapTask := c.mapTasks[key]
				mapTask.Status = "progress"
				mapTask.MachineID = args.MachineID
				reply.MapFileName = key
				reply.MapTaskID = c.mapWorkerID
				reply.NReduceTask = c.nReduceTask
				c.mapWorkerID++
				return nil
			}
		}
	}
	if len(c.reduceFinished) != c.nReduceTask && c.reduceWorkerID < c.nReduceTask {
		reply.MapFileName = ""
		reply.MapTaskID = -1
		reply.ReduceTaskID = c.reduceWorkerID
		c.reduceTasks[c.reduceWorkerID] = &ReduceTask{Status: "progress", ReduceWorkerID: c.reduceWorkerID, StartTime: time.Now()}
		c.reduceWorkerID++
		reply.NReduceTask = c.nReduceTask
		reply.ReduceFileList = c.mapFinished
		return nil
	}
	if len(c.reduceFinished) == c.nReduceTask {
		return nil
	}
	for key, value := range c.reduceTasks {
		if value.Status != "complete" && time.Now().Sub(value.StartTime).Seconds() >= 10 {
			fmt.Println("Restart Reduce Task!")
			reduceTask := c.reduceTasks[key]
			reduceTask.Status = "progress"
			reduceTask.ReduceWorkerID = c.reduceWorkerID
			reduceTask.StartTime = time.Now()
			reply.ReduceWorkerID = c.reduceWorkerID
			c.reduceWorkerID++
			reply.NReduceTask = c.nReduceTask
			reply.ReduceFileList = c.mapFinished
			reply.ReduceTaskID = key
			return nil
		}
	}
	return nil
}

func (c *Coordinator) AssignDone(args *DoneArgs, reply *DoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.MapTaskID != -1 {
		if c.mapTasks[args.MapFileName].Status != "complete" {
			c.mapFinished = append(c.mapFinished, args.MapTaskID)
			mapTask := c.mapTasks[args.MapFileName]
			mapTask.Status = "complete"
			mapTask.MapTaskID = args.MapTaskID
			return nil
		}
	}
	if args.ReduceTaskID != -1 {
		if c.reduceTasks[args.ReduceTaskID].Status != "complete" {
			c.reduceFinished = append(c.reduceFinished, args.ReduceTaskID)
			reduceTask := c.reduceTasks[args.ReduceTaskID]
			reduceTask.Status = "complete"
		}
		return nil
	}
	reply.Exit = false
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
	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := len(c.mapFinished) == len(c.mapTasks) && len(c.reduceFinished) == c.nReduceTask
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mapWorkerID = 0
	c.reduceWorkerID = 0
	c.nReduceTask = nReduce
	c.mapTasks = map[string]*MapTask{}
	c.mapFinished = []int{}
	c.reduceTasks = map[int]*ReduceTask{}
	c.reduceFinished = []int{}
	for _, name := range files {
		c.mapTasks[name] = &MapTask{Status: "idle", MachineID: -1, MapTaskID: -1}
	}

	c.server()
	return &c
}
