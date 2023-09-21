package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for Run(mapf, reducef) {
		time.Sleep(time.Second)
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

func Run(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) bool {
	args := AssignArgs{}
	reply := AssignReply{}
	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Assign", &args, &reply)
	if ok {
		if reply.MapTaskID != -1 {
			content := readFile(reply.MapFileName)
			kva := mapf(reply.MapFileName, content)
			reducedTasks := make([][]KeyValue, reply.NReduceTask)
			for _, pair := range kva {
				reduceId := ihash(pair.Key) % reply.NReduceTask
				reducedTasks[reduceId] = append(reducedTasks[reduceId], pair)
			}
			for i := 0; i < reply.NReduceTask; i++ {
				jsonData, err := json.Marshal(reducedTasks[i])
				if err != nil {
					log.Fatal(err)
				}
				// Write the JSON data to a file
				err = ioutil.WriteFile(fmt.Sprintf("mr-%d-%d", reply.MapTaskID, i),
					jsonData, 0644)
				if err != nil {
					log.Fatal(err)
				}
			}
			doneArgs := DoneArgs{MapTaskID: reply.MapTaskID, MapFileName: reply.MapFileName, ReduceTaskID: -1}
			doneReply := DoneReply{}
			ok := call("Coordinator.AssignDone", &doneArgs, &doneReply)
			if ok {
				return !doneReply.Exit
			} else {
				log.Fatal("Call Done Error!")
			}
		} else if reply.ReduceTaskID != -1 {
			intermediate := []KeyValue{}
			for _, idx := range reply.ReduceFileList {
				fileContent, err := ioutil.ReadFile(fmt.Sprintf("mr-%d-%d", idx, reply.ReduceTaskID))
				if err != nil {
					log.Fatal(err)
				}
				var data []KeyValue
				err = json.Unmarshal(fileContent, &data)
				if err != nil {
					log.Fatal(err)
				}
				intermediate = append(intermediate, data...)
			}
			sort.Sort(ByKey(intermediate))

			//oname := fmt.Sprintf("mr-out-tmp-%d", reply.ReduceWorkerID)
			//nname := fmt.Sprintf("mr-out-%d", reply.ReduceTaskID)
			oname := fmt.Sprintf("mr-out-%d", reply.ReduceTaskID)
			ofile, _ := os.Create(oname)
			//
			// call Reduce on each distinct key in intermediate[],
			// and print the result to mr-out-0.
			//
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}

			ofile.Close()
			doneArgs := DoneArgs{MapTaskID: -1, MapFileName: "", ReduceTaskID: reply.ReduceTaskID}
			doneReply := DoneReply{}
			//os.Rename(oname, nname)
			ok := call("Coordinator.AssignDone", &doneArgs, &doneReply)
			if ok {
				return !doneReply.Exit
			} else {
				log.Fatal("Call Done Error!")
			}
		}
	} else {
		fmt.Printf("call failed!\n")
	}
	return true
}

func readFile(filename string) string {
	file, err := os.Open(filename)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	return string(content)
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
