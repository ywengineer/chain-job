package job

import (
	"context"
	jsoniter "github.com/json-iterator/go"
	"go.uber.org/zap"
)

func init() {
	RegisterFilter("json", func(conf *FilterConf, ctx context.Context, log *zap.Logger) Filter {
		f := &JsonFilter{}
		f.init(conf, ctx, log)
		return f
	})
	RegisterFilter("json_array", func(conf *FilterConf, ctx context.Context, log *zap.Logger) Filter {
		f := &JsonArrayFilter{}
		f.init(conf, ctx, log)
		return f
	})
}

type JsonFilter struct {
	log         *zap.Logger
	conf        *FilterConf
	genId       bool
	fillMissing map[string]interface{}
}

func (jf *JsonFilter) init(conf *FilterConf, ctx context.Context, log *zap.Logger) {
	jf.log = log
	jf.conf = conf
	jf.genId = conf.GetBool("generateId")
	if m, ok := conf.Metadata["fillMissing"]; ok {
		jf.fillMissing = m.(map[string]interface{})
	}
	if jf.genId && gsf == nil {
		log.Panic("generate id feature need global snowflake worker. please ensure invoke method SetGlobalSnowflakeInfo")
	}
}

func (jf *JsonFilter) check(p *map[string]interface{}) {
	if jf.genId {
		if id, e := gsf.NextId(); e != nil {
			jf.log.Error("generate snowflake id failed for JsonFilter", zap.Error(e))
		} else {
			(*p)["id"] = id
		}
	}
	if len(jf.fillMissing) > 0 {
		for k, v := range jf.fillMissing {
			if _, ok := (*p)[k]; !ok {
				(*p)[k] = v
			}
		}
	}
}

func (jf *JsonFilter) DoFilter(message *TaskData) {
	switch message.Payload.(type) {
	case string:
		p := new(map[string]interface{})
		if e := jsoniter.UnmarshalFromString(message.Payload.(string), p); e == nil {
			jf.check(p)
			message.Payload = p
		} else {
			jf.log.Error("parse json data failed.", zap.Any("data", *message))
		}
	case []byte:
		p := new(map[string]interface{})
		if e := jsoniter.Unmarshal(message.Payload.([]byte), p); e == nil {
			jf.check(p)
			message.Payload = p
		} else {
			jf.log.Error("parse json data failed.", zap.Any("data", *message))
		}
	}
}

type JsonArrayFilter struct {
	log         *zap.Logger
	conf        *FilterConf
	genId       bool
	fillMissing map[string]interface{}
}

func (jaf *JsonArrayFilter) init(conf *FilterConf, ctx context.Context, log *zap.Logger) {
	jaf.log = log
	jaf.conf = conf
	jaf.genId = conf.GetBool("generateId")
	if m, ok := conf.Metadata["fillMissing"]; ok {
		jaf.fillMissing = m.(map[string]interface{})
	}
	if jaf.genId && gsf == nil {
		log.Panic("generate id feature need global snowflake worker. please ensure invoke method SetGlobalSnowflakeInfo")
	}
}

func (jaf *JsonArrayFilter) DoFilter(message *TaskData) {
	switch message.Payload.(type) {
	case string:
		p := new([]map[string]interface{})
		if e := jsoniter.UnmarshalFromString(message.Payload.(string), p); e == nil {
			if jaf.genId || len(jaf.fillMissing) > 0 {
				for _, v := range *p {
					jaf.check(&v)
				}
			}
			message.Payload = p
		} else {
			jaf.log.Error("parse json array data failed.", zap.Any("data", *message))
		}
	case []byte:
		p := new([]map[string]interface{})
		if e := jsoniter.Unmarshal(message.Payload.([]byte), p); e == nil {
			if jaf.genId || len(jaf.fillMissing) > 0 {
				for _, v := range *p {
					jaf.check(&v)
				}
			}
			message.Payload = p
		} else {
			jaf.log.Error("parse json array data failed.", zap.Any("data", *message))
		}
	}
}

func (jaf *JsonArrayFilter) check(p *map[string]interface{}) {
	if jaf.genId {
		if id, e := gsf.NextId(); e != nil {
			jaf.log.Error("generate snowflake id failed for JsonFilter", zap.Error(e))
		} else {
			(*p)["id"] = id
		}
	}
	if len(jaf.fillMissing) > 0 {
		for k, v := range jaf.fillMissing {
			if _, ok := (*p)[k]; !ok {
				(*p)[k] = v
			}
		}
	}
}
