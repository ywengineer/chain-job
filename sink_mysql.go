package job

import (
	"context"
	"github.com/ywengineer/g-util/sql"
	"go.uber.org/zap"
	"reflect"
)

func init() {
	RegisterSink("mysql", func(conf *SinkConf, ctx context.Context, log *zap.Logger) Sink {
		s := &SinkMySQL{}
		s.init(conf, ctx, log)
		return s
	})
}

type SinkMySQL struct {
	conf      *SinkConf
	log       *zap.Logger
	mysql     *sql.MySQL
	sqlMap    map[interface{}]interface{}
	sqlMapKey string
}

func (sm *SinkMySQL) init(conf *SinkConf, ctx context.Context, log *zap.Logger) {
	sm.conf = conf
	sm.log = log
	if sc, ok := conf.Metadata["sql"]; ok {
		sm.sqlMap = sc.(map[interface{}]interface{})
	} else {
		log.Panic("missing sql config for SinkMySQL")
	}
	sqlMapKey := conf.Metadata.GetString("sqlMapKey")
	if len(sqlMapKey) == 0 {
		log.Panic("messing sqlMapKey config for SinkMySQL")
	}
	sm.sqlMapKey = sqlMapKey
	//
	if conf.Metadata.GetBool("global") {
		if mysql == nil {
			log.Panic("global mysql client not set.")
		} else {
			sm.mysql = mysql
		}
	} else {
		sm.mysql = newMySQLClient(conf.Metadata, log)
	}
}

func (sm *SinkMySQL) DoSink(message *TaskData) {
	defer func() {
		if err := recover(); err != nil {
			sm.log.Error("catch panic event.", sm.tag(), zap.Any("data", *message))
		}
	}()
	tp := message.Metadata.GetString(sm.sqlMapKey)
	if len(tp) == 0 {
		sm.log.Error("missing sql map key meta", sm.tag(), zap.String("sqkMapKey", sm.sqlMapKey), zap.Any("data", *message))
		return
	}
	//
	if sqlStr, ok := sm.sqlMap[tp]; !ok {
		sm.log.Error("missing sql", sm.tag(), zap.String("key", tp), zap.Any("data", *message))
	} else {
		//
		sm.sink(message.Payload, sqlStr.(string), message)
	}
}

func (sm *SinkMySQL) sink(data interface{}, sqlStr string, message *TaskData) {
	//
	kind := reflect.TypeOf(data).Kind()
	switch kind {
	case reflect.Ptr:
		sm.sink(reflect.ValueOf(data).Elem().Interface(), sqlStr, message)
	case reflect.Slice:
		slice := data.([]map[string]interface{})
		tx := sm.mysql.GetConn().MustBegin()
		for _, m := range slice {
			if _, e := tx.NamedExec(sqlStr, m); e != nil {
				sm.log.Error("bind sql param failed", sm.tag(), zap.Any("data", m), zap.Error(e))
			}
		}
		if e := tx.Commit(); e != nil {
			_ = tx.Rollback()
			sm.log.Error("execute sql failed", sm.tag(), zap.String("sql", sqlStr), zap.Any("data", message))
		}
	case reflect.Map:
		if _, e := sm.mysql.GetConn().NamedExec(sqlStr, data.(map[string]interface{})); e != nil {
			sm.log.Error("execute sql failed", sm.tag(), zap.String("sql", sqlStr), zap.Any("data", message))
		}
	default:
		sm.log.Error("unknown message kind for sink mysql", sm.tag(), zap.Any("kind", kind.String()))
	}
}

func (sm *SinkMySQL) tag() zap.Field {
	return zap.String("tag", "SinkMySQL")
}
