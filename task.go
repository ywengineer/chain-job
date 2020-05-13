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

type Task struct {
	conf       TaskConf
	log        *zap.Logger
	ctx        context.Context
	stop       context.CancelFunc
	terminated bool
	source     Source
	filters    []Filter
	filterSize int
	sinks      []Sink
	sinkSize   int
	stopChan   chan bool
	runState   sync.Once
	stopMu     sync.Mutex
}

func (task *Task) addFilter(filter Filter) {
	task.filters = append(task.filters, filter)
}

func (task *Task) addSink(sink Sink) {
	task.sinks = append(task.sinks, sink)
}

func (task *Task) Run() {
	task.runState.Do(task._Run)
}

func (task *Task) _Run() {
	task.terminated = false
	task.conf.Threads = util.MaxInt(task.conf.Threads, 1)
	task.log.Info("task started.", zap.Any("desc", task.conf.Desc), zap.Int("thread", task.conf.Threads))
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
	task.log.Info("task finished.", zap.Any("desc", task.conf.Desc))
	close(task.stopChan)
}

func (task *Task) run() {
	for {
		select {
		case data, ok := <-task.source.Read():
			if ok {
				if task.filterSize > 0 {
					for _, filter := range task.filters {
						filter.DoFilter(data)
					}
				}
				if task.sinkSize > 0 {
					for _, sink := range task.sinks {
						sink.DoSink(data)
					}
				}
			} else {
				return
			}
		}
	}
}

func (task *Task) Stop() <-chan bool {
	task.stopMu.Lock()
	defer task.stopMu.Unlock()
	if !task.terminated {
		task.terminated = true
		task.stop()
	}
	return task.stopChan
}

func NewTask(conf TaskConf, parentCtx context.Context, log *zap.Logger) *Task {
	ctx, cancel := context.WithCancel(parentCtx)
	task := &Task{
		conf:       conf,
		ctx:        ctx,
		stop:       cancel,
		stopChan:   make(chan bool),
		log:        log,
		terminated: true,
		source:     newSource(&conf.Source, ctx, log),
	}
	// create filters
	for _, value := range conf.Filters {
		fc := value
		if f := newFilter(&fc, ctx, log); f != nil {
			task.addFilter(f)
			task.filterSize += 1
		}
	}
	// create sink
	for _, value := range conf.Sinks {
		fc := value
		if f := newSink(&fc, ctx, log); f != nil {
			task.addSink(f)
			task.sinkSize += 1
		}
	}
	return task
}
