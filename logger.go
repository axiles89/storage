package storage_db

import (
	"github.com/sirupsen/logrus"
	"os"
)

func GetLogger() *logrus.Logger {
	logrus.SetOutput(os.Stdout)
	return logrus.StandardLogger()
}
