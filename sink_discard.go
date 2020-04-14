package job

import (
	"context"
	"go.uber.org/zap"
)

func init() {
	RegisterSink("discard", func(conf *SinkConf, ctx context.Context, log *zap.Logger) Sink {
		s := &SinkDiscard{}
		s.init(conf, ctx, log)
		return s
	})
}

type SinkDiscard struct {
	conf *SinkConf
	log  *zap.Logger
}

func (sl *SinkDiscard) init(conf *SinkConf, ctx context.Context, log *zap.Logger) {
	sl.conf = conf
	sl.log = log
}

func (sl *SinkDiscard) DoSink(message *TaskData) {
	sl.log.Debug("discard data", sl.tag(), zap.Any("data", *message))
}

func (sl *SinkDiscard) tag() zap.Field {
	return zap.String("tag", "SinkDiscard")
}
