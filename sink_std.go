package job

import (
	"context"
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
}

func (sl *SinkLog) init(conf *SinkConf, ctx context.Context, log *zap.Logger) {
	sl.conf = conf
	sl.log = log
}

func (sl *SinkLog) DoSink(message *TaskData) {
	sl.log.Info("sink", zap.Any("data", *message))
}
