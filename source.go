package job

import (
	"context"
	"github.com/ywengineer/g-util/util"
	"go.uber.org/zap"
)

type SourceMaker func(conf *SourceConf) Source

var sourceMap = make(map[string]SourceMaker)

func RegisterSource(typ string, maker SourceMaker) {
	if _, ok := sourceMap[typ]; ok {
		util.Warn("source maker [%s] already exists.", typ)
	} else {
		sourceMap[typ] = maker
	}
}

func newSource(conf *SourceConf, ctx context.Context, log *zap.Logger) Source {
	if maker, ok := sourceMap[conf.Type]; ok {
		s := maker(conf)
		s.Init(conf, ctx, log)
		return s
	}
	util.Warn("source maker [%s] not found", conf.Type)
	return nil
}

type Source interface {
	Init(conf *SourceConf, ctx context.Context, log *zap.Logger)
	Read() <-chan *TaskData
	Terminated() <-chan bool
}
