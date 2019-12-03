package job

type KeyValueConf struct {
	Metadata map[string]interface{} `json:"metadata"`
}

type TaskConf struct {
	Source  SourceConf   `json:"source"`
	Filters []FilterConf `json:"filters"`
	Sink    SinkConf     `json:"sink"`
	Threads int          `json:"threads"`
}

func (src *KeyValueConf) GetString(key string) string {
	if v, ok := src.Metadata[key]; ok {
		return v.(string)
	}
	return ""
}

func (src *KeyValueConf) GetBool(key string) bool {
	if v, ok := src.Metadata[key]; ok {
		return v.(bool)
	}
	return false
}

func (src *KeyValueConf) GetInt(key string) int {
	return int(src.GetInt64(key))
}

func (src *KeyValueConf) GetInt64(key string) int64 {
	if v, ok := src.Metadata[key]; ok {
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

type SinkConf struct {
	Type string `json:"type"`
	KeyValueConf
}

type SourceConf struct {
	Type string `json:"type"`
	KeyValueConf
}

type FilterConf struct {
	Type string `json:"type"`
	KeyValueConf
}
