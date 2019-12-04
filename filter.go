package job

import (
	"context"
	"github.com/ywengineer/g-util/util"
	"go.uber.org/zap"
)

type FilterMaker func(conf *FilterConf, ctx context.Context, log *zap.Logger) Filter

var filterMap = make(map[string]FilterMaker)

func RegisterFilter(typ string, maker FilterMaker) {
	if _, ok := filterMap[typ]; ok {
		util.Warn("filter maker [%s] already exists.", typ)
	} else {
		filterMap[typ] = maker
	}
}

type Filter interface {
	DoFilter(message *TaskData)
}

func newFilter(conf *FilterConf, ctx context.Context, log *zap.Logger) Filter {
	if maker, ok := filterMap[conf.Type]; ok {
		s := maker(conf, ctx, log)
		return s
	}
	util.Warn("filter maker [%s] not found", conf.Type)
	return nil
}
