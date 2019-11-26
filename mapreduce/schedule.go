package mapreduce

import (
	"fmt"
	//"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	taskIdxs := make([]int, ntasks)
	finishNum := 0
	for i := 0; i < ntasks; i++ {
		taskIdxs[i] = i
	}
	var free_workers []string
	//var wg sync.WaitGroup
	//wg.Add(ntasks)
loop:
	for {
		select {
		case w := <-registerChan:
			free_workers = append(free_workers, w)
		default:
			//must have default
		}
		for len(free_workers) > 0 {
			if finishNum >= ntasks {
				break loop
			}
			if len(taskIdxs) == 0 {
				continue
			}
			currentTaskIdx := taskIdxs[0]
			taskIdxs = taskIdxs[1:]
			worker := free_workers[0]
			free_workers = free_workers[1:]
			file := ""
			if phase == mapPhase {
				file = mapFiles[currentTaskIdx]
			}
			args := DoTaskArgs{jobName, file, phase, currentTaskIdx, n_other}
			currentTaskIdx++
			go func(args DoTaskArgs) {
				res := call(worker, "Worker.DoTask", args, new(struct{}))
				if res == false {
					taskIdxs = append(taskIdxs, args.TaskNumber)
				} else {
					free_workers = append(free_workers, worker)
					finishNum++
				}
				//wg.Done()
			}(args)
		}
	}
	//wg.Wait()

	fmt.Printf("Schedule: %v done\n", phase)
}
