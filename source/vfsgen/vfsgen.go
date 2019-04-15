package vfsgen

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/golang-migrate/migrate/v4/source"
	"github.com/shurcooL/httpfs/vfsutil"
)

var (
	ErrNoFileSystem = errors.New("expected *http.FileSystem")
)

func init() {
	source.Register("vfsgen", &VFSDriver{})
}

type VFSDriver struct {
	path       string
	fs         http.FileSystem
	migrations *source.Migrations
}

func (v *VFSDriver) Open(url string) (source.Driver, error) {
	return nil, fmt.Errorf("not implemented")
}

func WithInstance(instance interface{}) (source.Driver, error) {
	if _, ok := instance.(http.FileSystem); !ok {
		return nil, ErrNoFileSystem
	}
	fs := instance.(http.FileSystem)
	v := &VFSDriver{
		path:       "<vfsgen>",
		fs:         fs,
		migrations: source.NewMigrations(),
	}

	files, err := vfsutil.ReadDir(fs, ".")
	if err != nil {
		return nil, fmt.Errorf("Unable to list migrations: %s", err)
	}

	for _, fi := range files {
		if fi.IsDir() {
			// "file" source doesn't recurse, so skip it here too
			continue
		}

		m, err := source.DefaultParse(fi.Name())
		if err != nil {
			// ignore files that we can't parse
			continue
		}
		if !v.migrations.Append(m) {
			return nil, fmt.Errorf("unable to parse file %s", fi.Name())
		}
	}

	return v, nil
}

func (v *VFSDriver) Close() error {
	// nothing to do here
	return nil
}

func (v *VFSDriver) First() (version uint, err error) {
	var ok bool
	if version, ok = v.migrations.First(); !ok {
		err = &os.PathError{
			Op:   "first",
			Path: v.path,
			Err:  os.ErrNotExist,
		}
	}

	return
}

func (v *VFSDriver) Prev(version uint) (prevVersion uint, err error) {
	var ok bool
	if prevVersion, ok = v.migrations.Prev(version); !ok {
		err = &os.PathError{
			Op:   fmt.Sprintf("prev for version %d", version),
			Path: v.path,
			Err:  os.ErrNotExist,
		}
	}

	return
}

func (v *VFSDriver) Next(version uint) (nextVersion uint, err error) {
	var ok bool
	if nextVersion, ok = v.migrations.Next(version); !ok {
		err = &os.PathError{
			Op:   fmt.Sprintf("next for version %d", version),
			Path: v.path,
			Err:  os.ErrNotExist,
		}
	}

	return
}

func (v *VFSDriver) ReadUp(version uint) (r io.ReadCloser, identifier string, err error) {
	m, ok := v.migrations.Up(version)
	r, identifier, err = v.read(version, m, ok)
	return
}

func (v *VFSDriver) ReadDown(version uint) (r io.ReadCloser, identifier string, err error) {
	m, ok := v.migrations.Down(version)
	r, identifier, err = v.read(version, m, ok)
	return
}

func (v *VFSDriver) read(version uint, m *source.Migration, ok bool) (r io.ReadCloser, identifier string, err error) {
	if ok {
		var file http.File
		file, err = v.fs.Open(m.Raw)
		if err != nil {
			return
		}

		identifier = m.Identifier
		r = file
	} else {
		err = &os.PathError{
			Op:   fmt.Sprintf("read version %d", version),
			Path: v.path,
			Err:  os.ErrNotExist,
		}
	}

	return
}
