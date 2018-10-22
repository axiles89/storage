package command

import (
	"os"
	"fmt"
	"github.com/pkg/errors"
)

func CreateSSTFile(dir string, fid int64) (*os.File, error) {
	f, err := os.OpenFile(fmt.Sprintf("%s/%d.sst", dir, fid), os.O_CREATE|os.O_RDWR|os.O_SYNC|os.O_EXCL, 0666)
	if err != nil {
		return nil, err
	}
	if err := syncDir(dir); err != nil {
		return nil, err
	}
	return f, nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return errors.Wrapf(err,"Failed to open %s for sync ", dir)
	}
	if err = d.Sync(); err != nil {
		return errors.Wrapf(err,"Failed to sync %s", dir)
	}
	if err = d.Close(); err != nil {
		return errors.Wrapf(err,"Failed to close %s", dir)
	}
	return nil
}
