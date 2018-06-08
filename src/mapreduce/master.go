package mapreduce

import (
	"container/list"
	"fmt"
	"time"
)

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
	workers := make([]string, 0)
	timeout := time.After(1000 * time.Millisecond)
L:
	for {
		select {
		case v := <-mr.registerChannel:
			workers = append(workers, v)
		case <-timeout:
			break L
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}
	done := make(chan bool)
	// Assign map jobs
	for i := 0; i < mr.nMap; i++ {
		go func(i int) {
			args := &DoJobArgs{File: mr.file, Operation: Map, JobNumber: i, NumOtherPhase: mr.nReduce}
			var reply DoJobReply
			call(workers[i%cap(workers)], "Worker.DoJob", args, &reply)
			done <- true
		}(i)
	}
	// Wait for them to complete
	for i := 0; i < mr.nMap; i++ {
		<-done
	}
	// Assign reduce jobs
	for i := 0; i < mr.nReduce; i++ {
		go func(i int) {
			args := &DoJobArgs{File: mr.file, Operation: Reduce, JobNumber: i, NumOtherPhase: mr.nMap}
			var reply DoJobReply
			call(workers[i%cap(workers)], "Worker.DoJob", args, &reply)
			done <- true
		}(i)
	}
	// Wait for them to complete
	for i := 0; i < mr.nReduce; i++ {
		<-done
	}
	// Final step merge
	mr.Merge()
	return mr.KillWorkers()
}
