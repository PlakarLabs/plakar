package agent

import (
	"fmt"
	"os"
	"os/exec"
	"sync"
	"time"
)

type SchedulerEvent interface {
	isSchedulerEvent()
}

type TaskCompleted struct {
	Name string
	Err  error
}

func (TaskCompleted) isSchedulerEvent() {}

type Scheduler struct {
	tasks      map[string]chan bool
	tasksMutex sync.Mutex
}

func NewScheduler() *Scheduler {
	return &Scheduler{
		tasks: make(map[string]chan bool),
	}
}

func (s *Scheduler) Cancel(taskID string) {
	s.tasksMutex.Lock()
	defer s.tasksMutex.Unlock()

	if _, ok := s.tasks[taskID]; ok {
		close(s.tasks[taskID])
		delete(s.tasks, taskID)
	}
}

func (s *Scheduler) Schedule(task Task, notify chan<- SchedulerEvent) {
	s.tasksMutex.Lock()
	defer s.tasksMutex.Unlock()

	if _, ok := s.tasks[task.Name]; ok {
		close(s.tasks[task.Name])
		delete(s.tasks, task.Name)
	}

	terminate := make(chan bool)
	s.tasks[task.Name] = terminate
	go func(_task Task) {
		<-time.After(time.Until(_task.StartAT))
		for {
			select {
			case <-terminate:
				return
			case <-time.After(_task.Interval):
				fmt.Printf("[%s] %s: %s\n", time.Now().UTC(), _task.Name, _task.Source)
				err := exec.Command(os.Args[0], "push", "-tag", _task.Name, _task.Source).Run()
				fmt.Printf("[%s] %s: done\n", time.Now().UTC(), _task.Name)
				notify <- TaskCompleted{Name: _task.Name, Err: err}
				if _task.Keep > 0 {
					now := time.Now()
					olderParam := now.Add(-_task.Keep).Format(time.RFC3339)
					exec.Command(os.Args[0], "rm", "-older", olderParam, "-tag", _task.Name).Run()
				}
			}
		}
	}(task)
}
