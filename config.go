package storage_db

type Config struct {
	FlushBufferSize             int     `json:"flushBufferSize"`
	WriteBufferSize             int     `json:"writeBufferSize"`
	MemtableSize                int     `json:"memtableSize"`
	FileNumCompactionTrigger    int     `json:"fileNumCompactionTrigger"`
	NumLevels                   int     `json:"numLevels"`
	MaxSizeAmplificationPercent int     `json:"maxSizeAmplificationpercent"`
	SizeRatio                   float32 `json:"sizeRatio"`
}

var DefaultConfig Config

func init() {
	DefaultConfig = Config{
		FlushBufferSize:             10,
		MemtableSize:                2,
		FileNumCompactionTrigger:    2,
		MaxSizeAmplificationPercent: 20,
		NumLevels:                   4,
		SizeRatio:                   0.1,
	}
	DefaultConfig.WriteBufferSize = 100
}
