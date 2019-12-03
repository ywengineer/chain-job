package job

import (
	"context"
	jsoniter "github.com/json-iterator/go"
	"go.uber.org/zap"
)

func init() {
	RegisterFilter("json", func(conf *FilterConf) Filter {
		return &JsonFilter{}
	})
	RegisterFilter("json_array", func(conf *FilterConf) Filter {
		return &JsonArrayFilter{}
	})
}

type JsonFilter struct {
	log  *zap.Logger
	conf *FilterConf
}

func (jf *JsonFilter) Init(conf *FilterConf, ctx context.Context, log *zap.Logger) {
	jf.log = log
	jf.conf = conf
}

func (jf *JsonFilter) DoFilter(message *TaskData) {
	switch message.Payload.(type) {
	case string:
		p := new(map[string]interface{})
		if e := jsoniter.UnmarshalFromString(message.Payload.(string), p); e == nil {
			message.Payload = p
		} else {
			jf.log.Error("parse json data failed.", zap.Any("data", *message))
		}
	case []byte:
		p := new(map[string]interface{})
		if e := jsoniter.Unmarshal(message.Payload.([]byte), p); e == nil {
			message.Payload = p
		} else {
			jf.log.Error("parse json data failed.", zap.Any("data", *message))
		}
	}
}

type JsonArrayFilter struct {
	log  *zap.Logger
	conf *FilterConf
}

func (jaf *JsonArrayFilter) Init(conf *FilterConf, ctx context.Context, log *zap.Logger) {
	jaf.log = log
	jaf.conf = conf
}

func (jaf *JsonArrayFilter) DoFilter(message *TaskData) {
	switch message.Payload.(type) {
	case string:
		p := new([]map[string]interface{})
		if e := jsoniter.UnmarshalFromString(message.Payload.(string), p); e == nil {
			message.Payload = p
		} else {
			jaf.log.Error("parse json array data failed.", zap.Any("data", *message))
		}
	case []byte:
		p := new([]map[string]interface{})
		if e := jsoniter.Unmarshal(message.Payload.([]byte), p); e == nil {
			message.Payload = p
		} else {
			jaf.log.Error("parse json array data failed.", zap.Any("data", *message))
		}
	}
}
