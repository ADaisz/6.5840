package mr

import (
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const Debug = false

func DPrintln(a ...interface{}) (n int, err error) {
	if Debug {
		log.Println(a...)
	}
	return
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Coordinator struct {
	// Your definitions here.
	state   string        // 记录处于哪个阶段
	nReduce int           // reduce任务数量
	nMap    int           // map任务数量	- 传参给 Task 用
	taskQue chan *Task    // 任务队列
	taskMap map[int]*Task // 任务列表
	mu      sync.Mutex    // 锁
}

type TaskType string

const (
	MAP      = "map"
	REDUCE   = "reduce"
	NO_TASK  = "no_task"
	QUIT     = "quit"
	TIME_OUT = 10 * time.Second
)

type Task struct {
	ID       int
	Type     TaskType
	FileName string
	NReduce  int
	NMap     int
	Deadline int64
}

// Your code here -- RPC handlers for the worker to call.

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
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := c.state == QUIT

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce: nReduce,
		nMap:    len(files),
		taskQue: make(chan *Task, int(math.Max(float64(len(files)), float64(nReduce)))),
		taskMap: make(map[int]*Task),
		mu:      sync.Mutex{},
		state:   MAP,
	}

	for idx, filename := range files {
		// 创建MapTask
		task := Task{
			ID:       idx,
			Type:     MAP,
			FileName: filename,
			NReduce:  c.nReduce,
			NMap:     c.nMap,
			Deadline: -1,
		}
		c.taskQue <- &task
		c.taskMap[idx] = &task
	}
	go c.detector()

	c.server()
	return &c
}

func (c *Coordinator) detector() {
	for {
		DPrintln("current task number ", len(c.taskMap))
		if len(c.taskMap) == 0 {
			c.changeState()
		} else {
			c.taskTimeout()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (c *Coordinator) changeState() {
	if c.state == MAP {
		// MAP阶段转换为REDUCE阶段 生成reduce任务
		c.constructTask(REDUCE)
	} else if c.state == REDUCE {
		// Reduce阶段完成转换为 QUIT
		c.constructTask(QUIT)
	} else if c.state == QUIT {
		log.Println("coordinator exit")
		os.Exit(0)
	}
}

func (c *Coordinator) constructTask(state string) {
	oldState := c.state
	c.state = state
	DPrintln(oldState, " =======================================>", state)
	for i := 0; i < c.nReduce; i++ {
		task := Task{
			ID:       i,
			Type:     TaskType(state),
			NReduce:  c.nReduce,
			NMap:     c.nMap,
			Deadline: -1,
		}
		c.taskQue <- &task
		c.taskMap[i] = &task
	}
}

func (c *Coordinator) taskTimeout() {
	for _, task := range c.taskMap {
		DPrintln(time.Now().Unix(), " ", task.Deadline)
		if (task.Deadline != -1) && (time.Now().Unix() > task.Deadline) {
			// 任务超时
			task.Deadline = -1
			c.taskQue <- task
			DPrintln(task)
		}
	}
}

// coordinator 监听 worker RPC调用，分配任务
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	if len(c.taskMap) != 0 {
		task := <-c.taskQue
		task.Deadline = time.Now().Add(TIME_OUT).Unix()
		reply.Task = *task
	} else {
		reply.Task = Task{Type: NO_TASK}
	}
	return nil
}

func (c *Coordinator) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	c.mu.Lock() // map parallelism test fail
	defer c.mu.Unlock()
	DPrintln("delete task ", args.ID)
	delete(c.taskMap, args.ID)

	return nil
}
