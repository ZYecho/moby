// +build linux

package ceph

import (
	"io/ioutil"
	"os"
	"path"
	"strconv"

	"github.com/docker/docker/daemon/graphdriver"
	"github.com/docker/docker/pkg/containerfs"
	"github.com/docker/docker/pkg/idtools"
	"github.com/docker/docker/pkg/mount"
	"github.com/sirupsen/logrus"
)

var (
	logger = logrus.WithField("storage-driver", "ceph")
)

func init() {
	graphdriver.Register("ceph", Init)
}

type Driver struct {
	home string
	*RbdSet
}

func Init(home string, options []string, uidMaps, gidMaps []idtools.IDMap) (graphdriver.Driver, error) {
	if err := os.MkdirAll(home, 0700); err != nil && !os.IsExist(err) {
		logger.Errorf("Rbd create home dir %s failed: %v", err)
		return nil, err
	}
	rbdSet, err := NewRbdSet(home, true, options)
	if err != nil {
		return nil, err
	}
	if err := mount.MakePrivate(home); err != nil {
		return nil, err
	}
	d := &Driver{
		RbdSet: rbdSet,
		home:   home,
	}

	return graphdriver.NewNaiveDiffDriver(d, uidMaps, gidMaps), nil
}

func (d *Driver) String() string {
	return "ceph"
}

func (d *Driver) CreateReadWrite(id, parent string, opts *graphdriver.CreateOpts) error {
	return d.Create(id, parent, opts)
}

func (d *Driver) Create(id, parent string, opts *graphdriver.CreateOpts) error {
	if err := d.RbdSet.AddDevice(id, parent); err != nil {
		return err
	}
	return nil

	return nil
}

func (d *Driver) Remove(id string) error {
	if !d.RbdSet.HasDevice(id) {
		return nil
	}
	if err := d.RbdSet.DeleteDevice(id); err != nil {
		return err
	}
	mountPoint := path.Join(d.home, "mnt", id)
	if err := os.RemoveAll(mountPoint); err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}

func (d *Driver) Get(id, mountLabel string) (containerfs.ContainerFS, error) {
	mp := path.Join(d.home, "mnt", id)
	if err := os.MkdirAll(mp, 0755); err != nil && !os.IsExist(err) {
		return nil, err
	}
	if err := d.RbdSet.MountDevice(id, mp, mountLabel); err != nil {
		return nil, err
	}
	rootFs := path.Join(mp, "rootfs")
	if err := os.MkdirAll(rootFs, 0755); err != nil && !os.IsExist(err) {
		d.RbdSet.UnmountDevice(id)
		return nil, err
	}
	idFile := path.Join(mp, "id")
	if _, err := os.Stat(idFile); err != nil && os.IsNotExist(err) {
		// Create an "id" file with the container/image id in it to help reconscruct this in case
		// of later problems
		if err := ioutil.WriteFile(idFile, []byte(id), 0600); err != nil {
			d.RbdSet.UnmountDevice(id)
			return nil, err
		}
	}

	return containerfs.NewLocalContainerFS(rootFs), nil
}

func (d *Driver) Put(id string) error {
	if err := d.RbdSet.UnmountDevice(id); err != nil {
		logger.Errorf("Warning: error unmounting device %s: %s", id, err)
		return err
	}
	return nil
}

func (d *Driver) Exists(id string) bool {
	return d.RbdSet.HasDevice(id)
}

func (d *Driver) Status() [][2]string {
	status := [][2]string{
		{"Pool Objects", ""},
	}
	return status
}

func (d *Driver) GetMetadata(id string) (map[string]string, error) {
	info := d.RbdSet.Devices[id]
	metadata := make(map[string]string)
	metadata["BaseHash"] = info.BaseHash
	metadata["DeviceSize"] = strconv.FormatUint(info.Size, 10)
	metadata["DeviceName"] = info.Device
	return metadata, nil
}

func (d *Driver) Cleanup() error {
	err := d.RbdSet.Shutdown()
	if err2 := mount.Unmount(d.home); err2 == nil {
		err = err2
	}
	return err
}
