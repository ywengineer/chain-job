package job

import (
	"context"
	"github.com/ywengineer/g-util/util"
	"go.uber.org/zap"
)

type SinkMaker func(conf *SinkConf, ctx context.Context, log *zap.Logger) Sink

var sinkMap = make(map[string]SinkMaker)

func RegisterSink(typ string, maker SinkMaker) {
	if _, ok := sinkMap[typ]; ok {
		util.Warn("sink maker [%s] already exists.", typ)
	} else {
		sinkMap[typ] = maker
	}
}

func newSink(conf *SinkConf, ctx context.Context, log *zap.Logger) Sink {
	if maker, ok := sinkMap[conf.Type]; ok {
		s := maker(conf, ctx, log)
		return s
	}
	util.Warn("sink maker [%s] not found", conf.Type)
	return nil
}

type Sink interface {
	DoSink(message *TaskData)
	Terminated() <-chan bool
}
