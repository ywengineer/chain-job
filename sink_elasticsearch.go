package job

import (
	"context"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/ywengineer/g-util/es"
	"github.com/ywengineer/g-util/util"
	"go.uber.org/zap"
	"go.uber.org/zap/buffer"
	"reflect"
	"strconv"
	"strings"
	"sync"
)

func init() {
	RegisterSink("elastic", func(conf *SinkConf, ctx context.Context, log *zap.Logger) Sink {
		s := &SinkES{}
		s.init(conf, ctx, log)
		return s
	})
}

var _es *esapi.API
var esMutex = sync.Mutex{}

const ConfAddress = "address"

func SetGlobalES(conf KeyValueConf, log *zap.Logger) {
	esMutex.Lock()
	defer esMutex.Unlock()
	if _es == nil && conf.Contains(ConfAddress) {
		_es = es.NewESClient(conf.GetStringSlice(ConfAddress), log)
	} else {
		util.Error("global elastic client already exists.", mysql.String())
	}
}

type SinkES struct {
	conf       *SinkConf
	log        *zap.Logger
	_es        *esapi.API
	indicesMap map[interface{}]interface{}
	indicesKey string
	ctx        context.Context
}

func (sm *SinkES) init(conf *SinkConf, ctx context.Context, log *zap.Logger) {
	sm.conf = conf
	sm.log = log
	sm.ctx = ctx
	if sc, ok := conf.Metadata["indicesMap"]; ok {
		sm.indicesMap = sc.(map[interface{}]interface{})
	} else {
		log.Panic("missing indices map config for SinkES")
	}
	indicesKey := conf.Metadata.GetString("indicesKey")
	if len(indicesKey) == 0 {
		log.Panic("messing indicesKey config for SinkES")
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

func (sm *SinkES) DoSink(message *TaskData) {
	tp := message.Metadata.GetString(sm.indicesKey)
	if len(tp) == 0 {
		sm.log.Error("missing indices key meta", zap.String("tag", "SinkES"), zap.String("indicesKey", sm.indicesKey), zap.Any("data", *message))
		return
	}
	//
	if indices, ok := sm.indicesMap[tp]; !ok {
		sm.log.Error("missing indices map", zap.String("tag", "SinkES"), zap.String("key", tp), zap.Any("data", *message))
	} else {
		//
		sm.sink(message.Payload, indices.(string), message)
	}
}

var bufPool = buffer.NewPool()

func (sm *SinkES) sink(data interface{}, indices string, message *TaskData) {
	//
	kind := reflect.TypeOf(data).Kind()
	switch kind {
	case reflect.Ptr:
		sm.sink(reflect.ValueOf(data).Elem().Interface(), indices, message)
	case reflect.Slice:
		slice := data.([]map[string]interface{})
		//
		buf := bufPool.Get()
		defer buf.Free()
		//
		for _, item := range slice {
			if id, ok := item["id"]; ok {
				docID := strconv.FormatUint(id.(uint64), 10)
				buf.AppendString(`{"index" : { "_index" : "` + indices + `", "_id" : "` + docID + `" }}`)
			} else {
				buf.AppendString(`{"index" : { "_index" : "` + indices + `" }}`)
			}
			itemJsonString, _ := jsonApi.MarshalToString(item)
			buf.AppendString(itemJsonString)
		}
		//
		bulk := sm._es.Bulk
		res, err := bulk(
			strings.NewReader(buf.String()),
			bulk.WithRefresh("true"),
			bulk.WithContext(sm.ctx),
		)
		//
		if err != nil || res.IsError() {
			sm.log.Error("execute insert failed", zap.String("tag", "SinkES"), zap.String("indices", indices), zap.Any("data", message))
		} else {
			_ = res.Body.Close()
		}
	case reflect.Map:
		if json, e := jsonApi.MarshalToString(data); e != nil {
			sm.log.Error("encode map to json failed.", zap.String("tag", "SinkES"), zap.String("indices", indices), zap.Any("data", message))
		} else {
			insert := sm._es.Index
			//
			var res *esapi.Response
			if id, ok := data.(map[string]interface{})["id"]; ok {
				docID := strconv.FormatUint(id.(uint64), 10)
				res, e = insert(indices, strings.NewReader(json), insert.WithDocumentID(docID), insert.WithContext(sm.ctx))
			} else {
				res, e = insert(indices, strings.NewReader(json), insert.WithContext(sm.ctx))
			}
			//
			if e != nil || res.IsError() {
				sm.log.Error("execute insert failed", zap.String("tag", "SinkES"), zap.String("indices", indices), zap.Any("data", message))
			} else {
				_ = res.Body.Close()
			}
		}
	default:
		sm.log.Error("unknown message kind for sink elastic", zap.Any("kind", kind.String()))
	}
}
