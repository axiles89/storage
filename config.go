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
	IndexFolder					string 	`json:"indexFolder"`
	MaxFileSize                 int     `json:"maxFileSize"`
	WalFolder				    string  `json:"walDirectory"`
	WalFileSize					int 	`json:"walFileSize"`
	WalBufferSize				int 	`json:"walBufferSize"`
}

var DefaultConfig Config

func init() {
	dir, _ := os.Getwd()
	DefaultConfig = Config{
		FlushBufferSize:             10,
		MemtableSize:                100,
		FileNumCompactionTrigger:    3,
		MaxSizeAmplificationPercent: 20,
		NumLevels:                   10,
		SizeRatio:                   0.1,
		DataFolder:                  dir + "/sst",
		IndexFolder:                 dir + "/sst/index",
		WalFolder:					 dir + "/wal",
		WalFileSize:				 20,
		WalBufferSize:				 10,
		MaxFileSize:                 1024 * 64,
	}
	DefaultConfig.WriteBufferSize = 100
}
