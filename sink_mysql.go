package job

import (
	"context"
	"github.com/ywengineer/g-util/sql"
	"github.com/ywengineer/g-util/util"
	"go.uber.org/zap"
	"reflect"
	"sync"
)

func init() {
	RegisterSink("mysql", func(conf *SinkConf, ctx context.Context, log *zap.Logger) Sink {
		s := &SinkMySQL{}
		s.init(conf, ctx, log)
		return s
	})
}

var mysql *sql.MySQL
var mysqlMutex = sync.Mutex{}

func SetGlobalMySQL(conf KeyValueConf, log *zap.Logger) {
	mysqlMutex.Lock()
	defer mysqlMutex.Unlock()
	if mysql == nil {
		mysql = newMySQLClient(conf, log)
	} else {
		util.Error("global mysql client already exists.", mysql.String())
	}
}

type SinkMySQL struct {
	conf      *SinkConf
	log       *zap.Logger
	stop      chan bool
	mysql     *sql.MySQL
	sqlMap    map[string]string
	sqlMapKey string
}

func (sm *SinkMySQL) init(conf *SinkConf, ctx context.Context, log *zap.Logger) {
	sm.conf = conf
	sm.log = log
	sm.stop = make(chan bool)
	if sc, ok := conf.Metadata["sql"]; ok {
		sm.sqlMap = sc.(map[string]string)
	} else {
		log.Panic("missing sql config for SinkMySQL")
	}
	sqlMapKey := conf.GetString("sqlMapKey")
	if len(sqlMapKey) == 0 {
		log.Panic("messing sqlMapKey config for SinkMySQL")
	}
	sm.sqlMapKey = sqlMapKey
	//
	if conf.GetBool("global") {
		if mysql == nil {
			log.Panic("global mysql client not set.")
		} else {
			sm.mysql = mysql
		}
	} else {
		sm.mysql = newMySQLClient(conf.KeyValueConf, log)
	}
	util.Watch(ctx, sm.stop)
}

func (sm *SinkMySQL) DoSink(message *TaskData) {
	tp := message.GetString(sm.sqlMapKey)
	if len(tp) == 0 {
		sm.log.Error("missing sql map key meta", zap.String("tag", "SinkMySQL"), zap.String("sqkMapKey", sm.sqlMapKey), zap.Any("data", *message))
		return
	}
	//
	sqlStr, ok := "", false
	if sqlStr, ok = sm.sqlMap[tp]; !ok {
		sm.log.Error("missing sql", zap.String("tag", "SinkMySQL"), zap.String("key", tp), zap.Any("data", *message))
		return
	}
	//
	kind := reflect.TypeOf(message.Payload).Kind()
	switch kind {
	case reflect.Slice:
		slice := message.Payload.([]map[string]interface{})
		tx := sm.mysql.GetConn().MustBegin()
		for _, m := range slice {
			_, _ = tx.NamedExec(sqlStr, m)
		}
		if e := tx.Commit(); e != nil {
			_ = tx.Rollback()
			sm.log.Error("execute sql failed", zap.String("tag", "SinkMySQL"), zap.String("sql", sqlStr), zap.Any("data", *message))
		}
	case reflect.Map:
		if _, e := sm.mysql.GetConn().NamedExec(sqlStr, message.Payload.(map[string]interface{})); e != nil {
			sm.log.Error("execute sql failed", zap.String("tag", "SinkMySQL"), zap.String("sql", sqlStr), zap.Any("data", *message))
		}
	default:
		sm.log.Error("unknown message kind for sink mysql", zap.Any("kind", kind.String()))
	}
}

func (sm *SinkMySQL) Terminated() <-chan bool {
	return sm.stop
}

func newMySQLClient(conf KeyValueConf, log *zap.Logger) *sql.MySQL {
	return sql.NewMySQL(conf.GetString("user"), conf.GetString("password"), conf.GetString("host"), conf.GetString("db"),
		conf.GetString("loc"), conf.GetInt("port"), conf.GetInt("writeTimeout"), conf.GetInt("readTimeout"), conf.GetInt("dialTimeout"),
		conf.GetInt("maxOpenConn"), conf.GetInt("maxIdleConn"), log)
}
