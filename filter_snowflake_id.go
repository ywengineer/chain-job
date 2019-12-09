package job

import (
	"context"
	"github.com/ywengineer/g-util/util"
	"github.com/ywengineer/snowflake-golang/pro"
	"go.uber.org/zap"
	"sync"
)

func init() {
	RegisterFilter("snowflake-id", func(conf *FilterConf, ctx context.Context, log *zap.Logger) Filter {
		f := &SnowflakeIDFilter{}
		f.init(conf, ctx, log)
		return f
	})
}

var gsf *pro.Worker
var mu = sync.Mutex{}

func SetGlobalSnowflakeInfo(center, machine uint64) {
	mu.Lock()
	defer mu.Unlock()
	if gsf == nil {
		gsf, _ = pro.NewWorker(center, machine)
	} else {
		util.Error("global snowflake worker already exists.", gsf.String())
	}
}

type SnowflakeIDFilter struct {
	log    *zap.Logger
	conf   *FilterConf
	worker *pro.Worker
}

func (sif *SnowflakeIDFilter) init(conf *FilterConf, ctx context.Context, log *zap.Logger) {
	sif.log = log
	sif.conf = conf
	// 如果是全局
	if conf.Metadata.GetBool("global") {
		if gsf == nil {
			log.Panic("global snowflake filter not set.")
		} else {
			sif.worker = gsf
		}
	} else {
		//
		if worker, err := pro.NewWorker(conf.Metadata.GetUInt64("center"), conf.Metadata.GetUInt64("machine")); err != nil {
			log.Panic("init snowflake uid filter failed.", zap.Error(err))
		} else {
			sif.worker = worker
		}
	}
}

func (sif *SnowflakeIDFilter) DoFilter(message *TaskData) {
	if id, e := sif.worker.NextId(); e != nil {
		sif.log.Error("generate snowflake id failed", zap.Error(e))
	} else {
		message.Metadata[MetaSnowflakeID] = id
	}
}
