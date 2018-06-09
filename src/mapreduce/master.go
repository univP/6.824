package mapreduce

import (
	"container/list"
	"fmt"
	"log"
	"sync"
	"time"
)

type WorkerInfo struct {
	address string
	// You can add definitions here.
	nJobs int
	mux   sync.Mutex
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

func (mr *MapReduce) RunMapJobs() {
	// Wait for atleast one worker to arrive
	w := <-mr.registerChannel
	mr.Workers[w] = &WorkerInfo{address: w, nJobs: 0}
	// Add all jobs to the channel
	jobs := make(chan int, mr.nMap)
	for i := 0; i < mr.nMap; i++ {
		jobs <- i
	}
	// Counter of completed jobs
	completed := 0
	// Channel to counter race conditions
	done := make(chan bool)
	// Mutex needed to access map
	var mux sync.Mutex
	// Greedy Choice: Least number of jobs
	for completed < mr.nMap {
		select {
		case w := <-mr.registerChannel:
			log.Printf("Worker %v arrived...\n", w)
			mr.Workers[w] = &WorkerInfo{address: w, nJobs: 0}
		case i := <-jobs:
			log.Printf("Map job %v assigned...\n", i)
			minv := mr.nMap + 1
			var minp *WorkerInfo
			mux.Lock()
			for _, v := range mr.Workers {
				if v.nJobs < minv {
					minv = v.nJobs
					minp = v
				}
			}
			mux.Unlock()
			if minp == nil {
				jobs <- i
				time.Sleep(10 * time.Millisecond)
				continue
			}
			go func(mr *MapReduce, i int, minp *WorkerInfo) {
				args := &DoJobArgs{
					File:          mr.file,
					Operation:     Map,
					JobNumber:     i,
					NumOtherPhase: mr.nReduce,
				}
				var reply DoJobReply
				minp.mux.Lock()
				minp.nJobs++
				minp.mux.Unlock()
				ok := call(minp.address, "Worker.DoJob", args, &reply)
				if ok {
					minp.mux.Lock()
					minp.nJobs--
					minp.mux.Unlock()
					done <- true
				} else {
					// Remove worker from set and add current job to queue
					mux.Lock()
					delete(mr.Workers, minp.address)
					mux.Unlock()
					jobs <- i
				}
			}(mr, i, minp)
		case <-done:
			completed++
			log.Printf("Completed %v Map jobs...\n", completed)
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (mr *MapReduce) RunReduceJobs() {
	// Add all jobs to the channel
	jobs := make(chan int, mr.nReduce)
	for i := 0; i < mr.nReduce; i++ {
		jobs <- i
	}
	// Counter of completed jobs
	completed := 0
	// Channel to counter race conditions
	done := make(chan bool)
	// Mutex needed to access map
	var mux sync.Mutex
	// Greedy Choice: Least number of jobs
	for completed < mr.nReduce {
		select {
		case w := <-mr.registerChannel:
			log.Printf("Worker %v arrived...\n", w)
			mr.Workers[w] = &WorkerInfo{address: w, nJobs: 0}
		case i := <-jobs:
			log.Printf("Reduce job %v assigned...\n", i)
			minv := mr.nReduce + 1
			var minp *WorkerInfo
			mux.Lock()
			for _, v := range mr.Workers {
				if v.nJobs < minv {
					minv = v.nJobs
					minp = v
				}
			}
			mux.Unlock()
			if minp == nil {
				jobs <- i
				time.Sleep(10 * time.Millisecond)
				continue
			}
			go func(mr *MapReduce, i int) {
				args := &DoJobArgs{
					File:          mr.file,
					Operation:     Reduce,
					JobNumber:     i,
					NumOtherPhase: mr.nMap,
				}
				var reply DoJobReply
				minp.mux.Lock()
				minp.nJobs++
				minp.mux.Unlock()
				ok := call(minp.address, "Worker.DoJob", args, &reply)
				if ok {
					minp.mux.Lock()
					minp.nJobs--
					minp.mux.Unlock()
					done <- true
				} else {
					// Remove worker from set and add current job to queue
					mux.Lock()
					delete(mr.Workers, minp.address)
					mux.Unlock()
					jobs <- i
				}
			}(mr, i)
		case <-done:
			completed++
			log.Printf("Completed %v Reduce jobs...\n", completed)
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	// Run map jobs
	mr.RunMapJobs()
	// Run Reduce Jobs
	mr.RunReduceJobs()
	// Final step merge
	mr.Merge()
	return mr.KillWorkers()
}
