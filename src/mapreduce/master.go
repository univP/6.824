package mapreduce

import "container/list"
import "fmt"

type WorkerInfo struct {
	address string
	// You can add definitions here.
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	// Listen to incoming registrations
	worker := <-mr.registerChannel
	// Assign map and reduce jobs
	for i := 0; i < mr.nMap; i++ {
		args := &DoJobArgs{File: mr.file, Operation: Map, JobNumber: i, NumOtherPhase: mr.nReduce}
		var reply DoJobReply
		call(worker, "Worker.DoJob", args, &reply)
	}
	for i := 0; i < mr.nReduce; i++ {
		args := &DoJobArgs{File: mr.file, Operation: Reduce, JobNumber: i, NumOtherPhase: mr.nMap}
		var reply DoJobReply
		call(worker, "Worker.DoJob", args, &reply)
	}
	// Final step merge
	mr.Merge()
	return mr.KillWorkers()
}
