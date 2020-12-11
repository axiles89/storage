package command

import (
	"os"
	"fmt"
	"github.com/pkg/errors"
)

func OpenWalFile(dir string, fid int64, flags int) (*os.File, error) {
	// O_EXCL
	f, err := os.OpenFile(fmt.Sprintf("%s/%d.bin", dir, fid), flags, 0666)
	if err != nil {
		return nil, err
	}
	if err := SyncDir(dir); err != nil {
		return nil, err
	}
	return f, nil
}

func OpenIdxFile(dir string, fid int64, flags int) (*os.File, error) {
	// O_EXCL
	f, err := os.OpenFile(fmt.Sprintf("%s/%d.idx", dir, fid), flags, 0666)
	if err != nil {
		return nil, err
	}
	if err := SyncDir(dir); err != nil {
		return nil, err
	}
	return f, nil
}

func OpenSSTFile(dir string, fid int64, flags int) (*os.File, error) {
	// O_EXCL
	f, err := os.OpenFile(fmt.Sprintf("%s/%d.sst", dir, fid), flags, 0666)
	if err != nil {
		return nil, err
	}
	if err := SyncDir(dir); err != nil {
		return nil, err
	}
	return f, nil
}

func SyncDir(dir string) error {
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

func DeleteSSTFile(dir string, fid int64) error {
	if err := os.Remove(fmt.Sprintf("%s/%d.sst", dir, fid)); err != nil {
		return err
	}
	SyncDir(dir)
	return nil
}

func DeleteIdxFile(dir string, fid int64) error {
	if err := os.Remove(fmt.Sprintf("%s/%d.idx", dir, fid)); err != nil {
		return err
	}
	SyncDir(dir)
	return nil
}
