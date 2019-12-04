package job

import (
	"context"
	"github.com/ywengineer/g-util/util"
	"go.uber.org/zap"
)

func init() {
	RegisterSink("log", func(conf *SinkConf, ctx context.Context, log *zap.Logger) Sink {
		s := &SinkLog{}
		s.init(conf, ctx, log)
		return s
	})
}

type SinkLog struct {
	conf *SinkConf
	log  *zap.Logger
	stop chan bool
}

func (sl *SinkLog) init(conf *SinkConf, ctx context.Context, log *zap.Logger) {
	sl.conf = conf
	sl.log = log
	sl.stop = make(chan bool)
	util.Watch(ctx, sl.stop)
}

func (sl *SinkLog) DoSink(message *TaskData) {
	sl.log.Info("sink", zap.Any("data", *message))
}

func (sl *SinkLog) Terminated() <-chan bool {
	return sl.stop
}
