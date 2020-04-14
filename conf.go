package job

import (
	jsoniter "github.com/json-iterator/go"
	"github.com/ywengineer/g-util/util"
	"gopkg.in/yaml.v2"
)

type KeyValueConf map[string]interface{}

type TaskConf struct {
	Desc    string       `json:"desc" yaml:"desc"`
	Source  SourceConf   `json:"source" yaml:"source"`
	Filters []FilterConf `json:"filters"  yaml:"filters"`
	Sink    SinkConf     `json:"sink" yaml:"sink"`
	Threads int          `json:"threads" yaml:"threads"`
}

func ParseConfFromYaml(data []byte) *[]TaskConf {
	t := new([]TaskConf)
	if e := yaml.Unmarshal(data, t); e == nil {
		return t
	} else {
		util.Panic("parse task conf from yaml failed. %v", e)
	}
	return nil
}

func ParseConfFromJson(data []byte) *[]TaskConf {
	t := new([]TaskConf)
	if e := jsoniter.Unmarshal(data, t); e == nil {
		return t
	} else {
		util.Panic("parse task conf from json failed. %v", e)
	}
	return nil
}

func (src *KeyValueConf) GetString(key string) string {
	if v, ok := (*src)[key]; ok {
		return v.(string)
	}
	return ""
}

func (src *KeyValueConf) GetStringOrDefault(key, def string) string {
	if v, ok := (*src)[key]; ok {
		return v.(string)
	}
	return def
}

func (src *KeyValueConf) GetBool(key string) bool {
	if v, ok := (*src)[key]; ok {
		return v.(bool)
	}
	return false
}

func (src *KeyValueConf) GetInt(key string) int {
	return int(src.GetInt64(key))
}

func (src *KeyValueConf) GetInt64(key string) int64 {
	if v, ok := (*src)[key]; ok {
		switch v.(type) {
		case int:
			return int64(v.(int))
		case int8:
			return int64(v.(int8))
		case int16:
			return int64(v.(int16))
		case int32:
			return int64(v.(int32))
		case int64:
			return v.(int64)
		}
	}
	return 0
}

func (src *KeyValueConf) GetStringSlice(key string) []string {
	if v, ok := (*src)[key]; ok {
		if r, ok := v.([]string); ok {
			return r
		} else {
			rc := make([]string, 0)
			for _, s := range v.([]interface{}) {
				rc = append(rc, s.(string))
			}
			return rc
		}
	}
	return nil
}

func (src *KeyValueConf) Contains(key string) bool {
	_, ok := (*src)[key]
	return ok
}

func (src *KeyValueConf) GetUInt64(key string) uint64 {
	if v, ok := (*src)[key]; ok {
		switch v.(type) {
		case int:
			return uint64(v.(int))
		case int8:
			return uint64(v.(int8))
		case int16:
			return uint64(v.(int16))
		case int32:
			return uint64(v.(int32))
		case int64:
			return uint64(v.(int64))
		case uint64:
			return v.(uint64)
		}
	}
	return 0
}

type SinkConf struct {
	Type     string       `json:"type" yaml:"type"`
	Metadata KeyValueConf `json:"metadata" yaml:"metadata"`
}

type SourceConf struct {
	Type     string       `json:"type" yaml:"type"`
	Metadata KeyValueConf `json:"metadata" yaml:"metadata"`
}

type FilterConf struct {
	Type     string       `json:"type" yaml:"type"`
	Metadata KeyValueConf `json:"metadata" yaml:"metadata"`
}
