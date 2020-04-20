package job

import (
	"context"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/ywengineer/g-util/es"
	"go.uber.org/zap"
	"reflect"
	"strconv"
	"strings"
)

func init() {
	RegisterSink("elastic_update", func(conf *SinkConf, ctx context.Context, log *zap.Logger) Sink {
		s := &SinkESUpdate{}
		s.init(conf, ctx, log)
		return s
	})
}

type SinkESUpdate struct {
	conf       *SinkConf
	log        *zap.Logger
	_es        *esapi.API
	indicesMap map[interface{}]interface{}
	indicesKey string
	ctx        context.Context
	docMap     map[interface{}]interface{}
}

func (sm *SinkESUpdate) init(conf *SinkConf, ctx context.Context, log *zap.Logger) {
	sm.conf = conf
	sm.log = log
	sm.ctx = ctx
	////////////////////////////////////////////////////////////////
	if sc, ok := conf.Metadata["indicesMap"]; ok {
		sm.indicesMap = sc.(map[interface{}]interface{})
	} else {
		log.Panic("missing indices map config for SinkESUpdate")
	}
	////////////////////////////////////////////////////////////////
	if sc, ok := conf.Metadata["docMap"]; ok {
		sm.docMap = sc.(map[interface{}]interface{})
	} else {
		log.Panic("missing doc map config for SinkESUpdate")
	}
	////////////////////////////////////////////////////////////////
	indicesKey := conf.Metadata.GetString("indicesKey")
	if len(indicesKey) == 0 {
		log.Panic("messing indicesKey config for SinkESUpdate")
	}
	sm.indicesKey = indicesKey
	//
	if conf.Metadata.GetBool("global") {
		if _es == nil {
			log.Panic("global elastic client not set.")
		} else {
			sm._es = _es
		}
	} else {
		sm._es = es.NewESClient(conf.Metadata.GetStringSlice(ConfAddress), log)
	}
}

func (sm *SinkESUpdate) DoSink(message *TaskData) {
	defer func() {
		if err := recover(); err != nil {
			sm.log.Error("catch panic event.", sm.tag(), zap.Any("data", *message))
		}
	}()
	tp := message.Metadata.GetString(sm.indicesKey)
	if len(tp) == 0 {
		sm.log.Error("missing indices key meta", sm.tag(), zap.String("indicesKey", sm.indicesKey), zap.Any("data", *message))
		return
	}
	//
	if indices, ok := sm.indicesMap[tp]; !ok {
		sm.log.Error("missing indices map", sm.tag(), zap.String("key", tp), zap.Any("data", *message))
	} else {
		//
		sm.sink(message.Payload, indices.(string), message)
	}
}

func (sm *SinkESUpdate) sink(data interface{}, indices string, message *TaskData) {
	//
	kind := reflect.TypeOf(data).Kind()
	switch kind {
	case reflect.Ptr:
		sm.sink(reflect.ValueOf(data).Elem().Interface(), indices, message)
	case reflect.Slice:
		slice := data.([]map[string]interface{})
		//
		buf := reqBodyBufPool.Get()
		defer buf.Free()
		//
		for _, item := range slice {
			if id, ok := item["id"]; ok {
				docID := strconv.FormatUint(id.(uint64), 10)
				//
				if doc := sm.parseData(item); len(doc) > 0 {
					buf.AppendString(`{"update" : { "_index" : "` + indices + `", "_id" : "` + docID + `" }}`)
					itemJsonString, _ := jsonApi.MarshalToString(doc)
					buf.AppendString("\n")
					buf.AppendString(`{"doc": ` + itemJsonString + `}`)
					buf.AppendString("\n")
				} else {
					sm.log.Error("missing data for update", sm.tag(), zap.Any("data", item))
				}
			} else {
				sm.log.Error("missing unique id for update", sm.tag(), zap.Any("data", item))
			}
		}
		// if has data to bulk
		if buf.Len() > 0 {
			//
			bulk := sm._es.Bulk
			res, err := bulk(
				strings.NewReader(buf.String()),
				bulk.WithRefresh("true"),
				bulk.WithContext(sm.ctx),
			)
			//
			if err != nil || res.IsError() {
				sm.log.Error("execute insert failed",
					sm.tag(),
					zap.Error(err),
					zap.String("indices", indices),
					zap.Any("data", message),
					zap.String("info", res.String()),
					zap.String("body", buf.String()))
			}
			if res != nil {
				_ = res.Body.Close()
			}
		}
	case reflect.Map:
		src := data.(map[string]interface{})
		if id, ok := src["id"]; ok {
			docID := strconv.FormatUint(id.(uint64), 10)
			//
			if doc := sm.parseData(src); len(doc) > 0 {
				//
				json, _ := jsonApi.MarshalToString(doc)
				//
				update := sm._es.Update
				//
				res, e := update(indices, docID, strings.NewReader(`{"doc": `+json+`}`), update.WithContext(sm.ctx))
				//
				if e != nil || res.IsError() {
					sm.log.Error("execute insert failed", sm.tag(), zap.String("indices", indices), zap.Any("data", message))
				}
				if res != nil {
					_ = res.Body.Close()
				}
			} else {
				sm.log.Error("missing data for update", sm.tag(), zap.Any("data", src))
			}
		} else {
			sm.log.Error("missing unique id for update", sm.tag(), zap.Any("data", src))
		}
	default:
		sm.log.Error("unknown message kind for sink elastic", sm.tag(), zap.Any("kind", kind.String()))
	}
}

func (sm *SinkESUpdate) tag() zap.Field {
	return zap.String("tag", "SinkESUpdate")
}

func (sm *SinkESUpdate) parseData(src map[string]interface{}) map[string]interface{} {
	m := make(map[string]interface{})
	//
	for k, v := range sm.docMap {
		if sv, ok := src[v.(string)]; ok {
			m[k.(string)] = sv
		}
	}
	return m
}
