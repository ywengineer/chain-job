package job

import (
	"context"
	v2 "github.com/ywengineer/snowflake-golang/v2"
	"go.uber.org/zap"
)

func init() {
	RegisterFilter("snowflake-id", func(conf *FilterConf) Filter {
		return &SnowflakeIDFilter{}
	})
}

type SnowflakeIDFilter struct {
	log    *zap.Logger
	conf   *FilterConf
	worker *v2.Worker
}

func (sif *SnowflakeIDFilter) Init(conf *FilterConf, ctx context.Context, log *zap.Logger) {
	sif.log = log
	sif.conf = conf
	if worker, err := v2.NewWorker(conf.GetUInt64("center"), conf.GetUInt64("machine")); err != nil {
		log.Panic("init snowflake uid filter failed.", zap.Error(err))
	} else {
		sif.worker = worker
	}
}

func (sif *SnowflakeIDFilter) DoFilter(message *TaskData) {
	if id, e := sif.worker.NextId(); e != nil {
		sif.log.Error("generate snowflake id failed", zap.Error(e))
	} else {
		message.Metadata[MetaSnowflakeID] = id
	}
}
