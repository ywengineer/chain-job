package job

import (
	"context"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	jsoniter "github.com/json-iterator/go"
	"github.com/ywengineer/g-util/es"
	"github.com/ywengineer/g-util/sql"
	"go.uber.org/zap"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"time"
)

func init() {
	RegisterFilter("elastic_analyzer", func(conf *FilterConf, ctx context.Context, log *zap.Logger) Filter {
		s := &FilterESAnalyzer{}
		s.init(conf, ctx, log)
		return s
	})
}

type FilterESAnalyzer struct {
	conf *FilterConf
	log  *zap.Logger
	_es  *esapi.API
	ctx  context.Context
	//
	indices  string
	analyzer string
	//
	timeKey string
	props   []string
	notify  string
}

func (sm *FilterESAnalyzer) init(conf *FilterConf, ctx context.Context, log *zap.Logger) {
	sm.conf = conf
	sm.log = log
	sm.ctx = ctx
	sm.timeKey = conf.Metadata.GetString("timeKey")
	sm.notify = conf.Metadata.GetString("notifyUrl")
	if len(sm.notify) > 0 {
		if _, err := url.ParseRequestURI(sm.notify); err != nil {
			log.Fatal("parse notify url error", sm.tag(), zap.Error(err), zap.String("url", sm.notify))
		}
	}
	//
	if v := conf.Metadata.GetString("indices"); len(v) > 0 {
		sm.indices = v
	} else {
		log.Fatal("indices metadata must be set for filter elastic_analyzer", sm.tag())
	}
	if v := conf.Metadata.GetString("analyzer"); len(v) > 0 {
		sm.analyzer = v
	} else {
		log.Fatal("analyzer metadata must be set for filter elastic_analyzer", sm.tag())
	}
	if v := conf.Metadata.GetStringSlice("props"); len(v) > 0 {
		sm.props = v
	} else {
		log.Fatal("props metadata must be set for filter elastic_analyzer", sm.tag())
	}
	//
	if conf.Metadata.GetBool("global") {
		if _es == nil {
			log.Panic("global elastic client not set.", sm.tag())
		} else {
			sm._es = _es
		}
	} else {
		sm._es = es.NewESClient(conf.Metadata.GetStringSlice(ConfAddress), log)
	}
}

func (sm *FilterESAnalyzer) DoFilter(message *TaskData) {
	//
	sm.sink(message.Payload, message)
}

func (sm *FilterESAnalyzer) sink(data interface{}, message *TaskData) {
	//
	kind := reflect.TypeOf(data).Kind()
	switch kind {
	case reflect.Ptr:
		sm.sink(reflect.ValueOf(data).Elem().Interface(), message)
	case reflect.Slice:
		slice := data.([]map[string]interface{})
		//
		buf := reqBodyBufPool.Get()
		defer buf.Free()
		//
		timeValue := ""
		//
		for _, item := range slice {
			//
			if len(timeValue) == 0 {
				timeValue = sm.extractTime(item)
			}
			buf.AppendString(sm.extract(item))
		}
		//
		words := sm.analysis(buf.String())
		//
		sm._notify(words, timeValue)
	case reflect.Map:
		//
		d := data.(map[string]interface{})
		//
		words := sm.analysis(sm.extract(d))
		//
		sm._notify(words, sm.extractTime(d))
	default:
		sm.log.Error("unknown message kind for filter elastic_analyzer", zap.Any("kind", kind.String()), sm.tag())
	}
}

func (sm *FilterESAnalyzer) extract(data map[string]interface{}) string {
	txt := ""
	for _, prop := range sm.props {
		if v, ok := data[prop]; ok {
			txt += v.(string)
		}
	}
	return txt
}

func (sm *FilterESAnalyzer) extractTime(data map[string]interface{}) string {
	if t, ok := data[sm.timeKey]; ok {
		return t.(string)
	} else {
		return time.Now().Format(sql.TimeFormat)
	}
}

func (sm *FilterESAnalyzer) _notify(words []string, time string) {
	// no words need to notify
	if len(words) <= 0 {
		return
	}
	if len(sm.notify) > 0 {
		body, _ := jsonApi.MarshalToString(words)
		req, err := http.NewRequestWithContext(sm.ctx, "POST", sm.notify, strings.NewReader(`{"time":"`+time+`", "words":`+body+`}`))
		if err != nil {
			sm.log.Error("create notify request error", sm.tag(), zap.Error(err))
			return
		}
		//
		req.Header.Add("Content-Type", "application/json;charset=utf-8")
		//
		res, err := transport.RoundTrip(req)
		//
		if err != nil || res.StatusCode > 299 {
			sm.log.Error("notify error", sm.tag(), zap.Error(err), zap.Any("data", words))
		} else {
			_ = res.Body.Close()
		}
	} else {
		sm.log.Info("analyze result", sm.tag(), zap.Any("words", words), zap.String("time", time))
	}
}

func (sm *FilterESAnalyzer) analysis(text string) []string {
	analyzer := sm._es.Indices.Analyze
	res, err := analyzer(analyzer.WithIndex(sm.indices),
		analyzer.WithContext(context.Background()),
		analyzer.WithBody(strings.NewReader(`{"analyzer":"`+sm.analyzer+`","text":"`+text+`"}`)))
	var words []string
	//
	if err != nil {
		sm.log.Error("Error getting analyzer response", zap.Error(err), sm.tag())
	} else if res.IsError() {
		sm.log.Error("analyzer response error", zap.String("status", res.String()), sm.tag())
	} else {
		// Deserialize the response into a map.
		var r map[string]interface{}
		if err := jsoniter.NewDecoder(res.Body).Decode(&r); err != nil {
			sm.log.Error("Error parsing the analyzer response body", zap.Error(err), sm.tag())
		} else {
			//
			if tokens, ok := r["tokens"]; ok {
				for _, token := range tokens.([]interface{}) {
					words = append(words, token.(map[string]interface{})["token"].(string))
				}
			}
		}
	}
	if res != nil {
		_ = res.Body.Close()
	}
	return words
}

func (sm *FilterESAnalyzer) tag() zap.Field {
	return zap.String("tag", "FilterESAnalyzer")
}
