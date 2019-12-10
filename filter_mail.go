package job

import (
	"context"
	jsoniter "github.com/json-iterator/go"
	"github.com/ywengineer/g-util/util"
	"go.uber.org/zap"
	"reflect"
)

func init() {
	RegisterFilter("mail", func(conf *FilterConf, ctx context.Context, log *zap.Logger) Filter {
		f := &SnowflakeIDFilter{}
		f.init(conf, ctx, log)
		return f
	})
}

type MailFilter struct {
	log       *zap.Logger
	conf      *FilterConf
	cond      string
	condValue string
	mc        *util.MailClient
}

func (mf *MailFilter) init(conf *FilterConf, ctx context.Context, log *zap.Logger) {
	mf.log = log
	mf.conf = conf
	mf.cond = conf.Metadata.GetString("cond")
	mf.condValue = conf.Metadata.GetString("condProp")
	if len(mf.cond) == 0 {
		log.Panic("missing metadata cond of MailFilter")
	}
	if len(mf.condValue) == 0 {
		log.Panic("missing metadata condProp of MailFilter")
	}
	if mc, e := util.NewMailSender(mf.conf.Metadata.GetString("host"),
		mf.conf.Metadata.GetInt("port"),
		mf.conf.Metadata.GetString("username"),
		mf.conf.Metadata.GetString("password")); e != nil {
		log.Panic("create mail client failed for mail filter.", zap.Any("info", conf.Metadata))
	} else {
		mf.mc = mc
	}
}

func (mf *MailFilter) DoFilter(message *TaskData) {
	mf.filter(message.Payload)
}

func (mf *MailFilter) matchCond(value interface{}) bool {
	if value == nil {
		return true
	}
	switch mf.cond {
	case "isEmpty":
		if v, ok := value.(string); ok {
			return len(v) == 0
		}
		return false
	default:
		mf.log.Warn("unsupported cond for mail filter", zap.String("cond", mf.cond))
		return false
	}
}

func (mf *MailFilter) sendMail(data map[string]interface{}) {
	//host string, port int, username, password string, from, to, cc, bcc string, subject, bodyType, bodyString string
	if d, e := jsoniter.MarshalToString(data); e == nil {
		go mf.mc.SendMail(
			mf.conf.Metadata.GetString("from"),
			mf.conf.Metadata.GetString("to"),
			mf.conf.Metadata.GetString("cc"),
			mf.conf.Metadata.GetString("bcc"),
			mf.conf.Metadata.GetString("subject"),
			mf.conf.Metadata.GetStringOrDefault("bodyType", "text/plain"),
			d,
		)
	}
}

func (mf *MailFilter) check(data map[string]interface{}) {
	if mf.matchCond(data[mf.condValue]) {
		mf.sendMail(data)
	}
}

func (mf *MailFilter) filter(data interface{}) {
	//
	kind := reflect.TypeOf(data).Kind()
	switch kind {
	case reflect.Ptr:
		mf.filter(reflect.ValueOf(data).Elem().Interface())
	case reflect.Slice:
		slice := data.([]map[string]interface{})
		for _, m := range slice {
			mf.check(m)
		}
	case reflect.Map:
		m := data.(map[string]interface{})
		mf.check(m)
	default:
		mf.log.Error("unknown message kind for mail filter", zap.Any("kind", kind.String()))
	}
}
