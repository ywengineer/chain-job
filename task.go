package job

import (
	"context"
	"github.com/ywengineer/g-util/util"
	"go.uber.org/zap"
	"sync"
)

type TaskData struct {
	Payload  interface{}
	Metadata KeyValueConf
}

type task struct {
	conf       *TaskConf
	mtx        sync.Mutex
	log        *zap.Logger
	ctx        context.Context
	stop       context.CancelFunc
	terminated bool
	source     Source
	filters    []Filter
	sink       Sink
	filterSize int
	stopChan   chan bool
}

func (task *task) addFilter(filter Filter) {
	task.filters = append(task.filters, filter)
}

func (task *task) Run() {
	task.mtx.Lock()
	defer task.mtx.Unlock()
	if !task.terminated {
		return
	}
	task.terminated = false
	task.conf.Threads = util.MaxInt(task.conf.Threads, 1)
	//
	wg := sync.WaitGroup{}
	wg.Add(task.conf.Threads)
	//
	for i := 0; i < task.conf.Threads; i++ {
		go func(wg *sync.WaitGroup) {
			defer wg.Done()
			//
			task.run()
		}(&wg)
	}
	//
	wg.Wait()
	//
	<-task.source.Terminated()
	<-task.sink.Terminated()
	task.log.Info("task finished.", zap.Any("config", *task.conf))
	close(task.stopChan)
}

func (task *task) run() {
	for {
		select {
		case data, ok := <-task.source.Read():
			if ok {
				if task.filterSize > 0 {
					for _, filter := range task.filters {
						filter.DoFilter(data)
					}
				}
				task.sink.DoSink(data)
			} else {
				return
			}
		}
	}
}

func (task *task) Stop() <-chan bool {
	task.mtx.Lock()
	defer task.mtx.Unlock()
	if !task.terminated {
		task.terminated = true
		task.stop()
	}
	return task.stopChan
}

func NewTask(conf *TaskConf, parentCtx context.Context, log *zap.Logger) *task {
	ctx, cancel := context.WithCancel(parentCtx)
	task := &task{
		conf:       conf,
		ctx:        ctx,
		stop:       cancel,
		stopChan:   make(chan bool),
		log:        log,
		terminated: true,
		source:     newSource(&conf.Source, ctx, log),
		sink:       newSink(&conf.Sink, ctx, log),
	}
	for _, value := range conf.Filters {
		if f := newFilter(&value, ctx, log); f != nil {
			task.addFilter(f)
			task.filterSize += 1
		}
	}
	return task
}
