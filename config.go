package storage_db

import "os"

type Config struct {
	FlushBufferSize             int     `json:"flushBufferSize"`
	WriteBufferSize             int     `json:"writeBufferSize"`
	MemtableSize                int     `json:"memtableSize"`
	FileNumCompactionTrigger    int     `json:"fileNumCompactionTrigger"`
	NumLevels                   int     `json:"numLevels"`
	MaxSizeAmplificationPercent int     `json:"maxSizeAmplificationpercent"`
	SizeRatio                   float32 `json:"sizeRatio"`
	DataFolder                  string  `json:"dataFolder"`
	MaxFileSize                 int     `json:"maxFileSize"`
}

var DefaultConfig Config

func init() {
	dir, _ := os.Getwd()
	DefaultConfig = Config{
		FlushBufferSize:             10,
		MemtableSize:                8000,
		FileNumCompactionTrigger:    5,
		MaxSizeAmplificationPercent: 20,
		NumLevels:                   10,
		SizeRatio:                   0.1,
		DataFolder:                  dir + "/sst",
		MaxFileSize:                 10000,
	}
	DefaultConfig.WriteBufferSize = 70
}
