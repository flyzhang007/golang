package processor

import (
	"net/http"
	"librados/rados"
	"fmt"
	"os"
	"librados/rbd"
	"strconv"
	"encoding/json"
	"os/exec"
	"time"
	"bytes"
	"db"
	"utils"
	"image"
)

const MaxProcessorNumber = 16

type Volume struct {
	size       uint64
	name       string
	parentPool string
	devPath    string
	fullname   string
}

func NewVolume(name string, pool string, size uint64) *Volume {
	return &Volume{
		size: size,
		name: name,
		parentPool: pool,
		devPath: "",
		fullname: "",
	}
}

func (volume *Volume) SetDevicePath(path string) {
	volume.devPath = path
}

func (volume *Volume) SetFullname(poolid int64, prefix string) {
	volume.fullname = string(poolid) + "." + prefix
}

func (volume *Volume) Create() error {
	conn, ioctx, err := NewConnAndOpenPool(volume.parentPool)
	defer DisConnAndClosePool(conn, ioctx)
	if err != nil {
		return GetError(statusCreateVolumeErr)
	}

	//TODO: 映象的feature设置
	image, err := rbd.Create(ioctx, volume.name, volume.size,22, 1)
	if err != nil {
		fmt.Fprintf(os.Stderr, "RBD create volume failed: %v\n", err)
		return GetError(statusCreateVolumeErr)
	}

	if err := image.Open(); err != nil {
		fmt.Fprintf(os.Stderr, "Open image failed: %v\n", err)
		return GetError(statusCreateVolumeErr)
	}
	defer image.Close()

	info, err := image.Stat()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Stat image failed:  %v\n", err)
		return GetError(statusCreateVolumeErr)
	}

	volume.SetFullname(ioctx.GetPoolID(), info.Block_name_prefix)

	return nil
}

func (volume *Volume)Remove() error {
	conn, ioctx, err := NewConnAndOpenPool(volume.parentPool)
	defer DisConnAndClosePool(conn, ioctx)
	if err != nil {
		return GetError(statusCreateVolumeErr)
	}

	img := rbd.GetImage(ioctx, volume.name)
	return img.Remove()
}

//rbd map iscsi -p rbd
//TODO: 命令路径
func (volume *Volume)Map() error {
	cmd := exec.Command("/home/wu_chao/ceph-10.2.9/src/rbd", "map", volume.name, "-p", volume.parentPool)
	var stdout bytes.Buffer
	cmd.Stdout = &stdout

	c := make(chan bool, 1)
	go func(ch chan bool, command *exec.Cmd) {
		if err := command.Run(); err != nil {
			fmt.Fprintf(os.Stderr, "Run volume map command failed: %v\n", err)
			ch <- false
		} else {
			ch <- true
		}
	}(c, cmd)

	select {
		case result := <- c:
			if result {
				volume.SetDevicePath(string(stdout.Bytes()[:len(stdout.Bytes()) - 1]))
				return nil
			} else {
				return GetError(statusUnmapVolumeErr)
			}
		case <-time.After(5 * time.Second):
			fmt.Fprintf(os.Stderr, "Run volume map command timeout\n")
			return GetError(statusTimeoutErr)
	}
}

func (volume *Volume)Unmap() error {
	cmd := exec.Command("/home/wu_chao/ceph-10.2.9/src/rbd", "unmap", volume.name, "-p", volume.parentPool)
	c := make(chan bool, 1)
	go func (ch chan bool, command *exec.Cmd) {
		if err := command.Run(); err != nil {
			fmt.Fprintf(os.Stderr, "Run volume unmap command failed: %v\n", err)
			ch <- false
		} else {
			ch <- true
		}
	}(c, cmd)

	select {
		case result := <- c:
			if result {
				return nil
			} else {
				return GetError(statusUnmapVolumeErr)
			}
		case <- time.After(5 * time.Second):
			fmt.Fprintf(os.Stderr, "Run volume unmap command timeout\n")
			return GetError(statusTimeoutErr)
	}
}

func (volume *Volume)UpdateCreateResult() error {
	t := utils.CurrentTime()
	handle := db.GetDBHandler()
	_, err := handle.Exec("INSERT INTO %s VALUES (%s, %s, %d, %s, %s, %s, %s)",
			db.VolumesTab, volume.parentPool, volume.name, volume.size, volume.devPath, volume.fullname, t, t)
	if err != nil {
		return err
	}
	return nil
}

/*
GET /?Action=InfoVolume&PoolName={PoolName|*} HTTP/1.1
Host: xxx.xxx.xxx.xxx
Date: GMT Date
Authorization: (optional)TODO
--------------------------
HTTP /1.1 200 OK
Server: dhcc.ebs
Date: GMT Date
Content-Type: application/json
Content-Length: n

n bytes json result
*/
func InfoVolumes(w http.ResponseWriter, r *http.Request) {
	pool := r.FormValue("PoolName")
	if pool == "" {
		fmt.Fprintf(os.Stderr, "Invalid Request: %v\n", r.RequestURI)
		SendStatus(w, http.StatusBadRequest, http.StatusText(http.StatusBadRequest))
		return
	}

	conn, err := rados.NewConn()
	if err != nil {
		fmt.Fprintf(os.Stderr, "NewConn failed: %v\n", err)
		SendStatus(w, statusInfoVolumesErr, "")
		return
	}
	defer conn.Shutdown()

	if err := conn.ReadDefaultConfigFile(); err != nil {
		fmt.Fprintf(os.Stderr, "ReadDefaultConfigFile failed: %v\n", err)
		SendStatus(w, statusInfoVolumesErr, "")
		return
	}

	if err := conn.Connect(); err != nil {
		fmt.Fprintf(os.Stderr, "Connect failed: %v\n", err)
		SendStatus(w, statusInfoVolumesErr, "")
		return
	}

	var pools []string
	if pool == "*" {
		if pools, err = conn.ListPools(); err != nil {
			fmt.Fprintf(os.Stderr, "ListPools failed: %v\n", err)
			SendStatus(w, statusInfoVolumesErr, "")
			return
		}
	} else {
		if err := conn.LookupPool(pool); err != nil {
			fmt.Fprintf(os.Stderr, "LookupPool failed, pool name:%v, %v\n", pool, err)
			SendStatus(w, statusNotFoundErr, "")
			return
		}
		pools = append(pools, pool)
	}

	imageInfo := make(map[string][]map[string]rbd.ImageInfo)
	for i := 0; i < len(pools); i++ {
		ioctx, err := conn.OpenIOContext(pools[i])
		if err != nil {
			fmt.Fprintf(os.Stderr, "OpenIOContext failed, pool name:%v, %v\n", pool, err)
			SendStatus(w, statusInfoVolumesErr, "")
			return
		}
		defer ioctx.Destroy()

		images, err := rbd.GetImageNames(ioctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "GetImageNames failed: %v\n", err)
			SendStatus(w, statusInfoVolumesErr, "")
			return
		}

		var imageList []map[string]rbd.ImageInfo
		for j := 0; j < len(images); j++ {
			image := rbd.GetImage(ioctx, images[j])
			if err := image.Open(); err != nil {
				fmt.Fprintf(os.Stderr, "Open failed, image name:%v/%v, %v\n", pool, images[j], err)
				SendStatus(w, statusInfoVolumesErr, "")
				return
			}
			defer image.Close()

			info, err := image.Stat()
			if err != nil {
				fmt.Fprintf(os.Stderr, "Stat failed, image name:%v/%v, %v\n", pool, images[j], err)
				SendStatus(w, statusInfoVolumesErr, "")
				return
			}
			infoPair := make(map[string]rbd.ImageInfo);infoPair[images[j]] = *info
			imageList = append(imageList, infoPair)
		}
		imageInfo[pools[i]] = imageList
	}

	payload, err := json.Marshal(imageInfo)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Encode payload failed: %v\n", err)
		SendStatus(w, statusInfoVolumesErr, "")
		return
	}

	fmt.Fprintf(os.Stderr, "payload: %v\n", string(payload))
	SendResponse(w, http.StatusOK, string(payload))
}

/*
GET /?Action=DelVolume&PoolName={PoolName}&VolumeName={volumeName} HTTP/1.1
Host: xxx.xxx.xxx.xxx
Date: GMT Date
Authorization: (optional)TODO
--------------------------
HTTP /1.1 200 OK
Server: dhcc.ebs
Date: GMT Date

OK
*/
func DelVolume(w http.ResponseWriter, r *http.Request) {
	pool := r.FormValue("PoolName")
	volume := r.FormValue("VolumeName")
	if pool == "" || volume == "" {
		fmt.Fprintf(os.Stderr, "Invalid Request: %v\n", r.RequestURI)
		SendStatus(w, http.StatusBadRequest, http.StatusText(http.StatusBadRequest))
		return
	}

	conn, err := rados.NewConn()
	if err != nil {
		fmt.Fprintf(os.Stderr, "NewConn failed: %v\n", err)
		SendStatus(w, statusDelVolumeErr, "")
		return
	}
	defer conn.Shutdown()

	if err := conn.ReadDefaultConfigFile(); err != nil {
		fmt.Fprintf(os.Stderr, "ReadDefaultConfigFile failed: %v\n", err)
		SendStatus(w, statusDelVolumeErr, "")
		return
	}

	if err := conn.Connect(); err != nil {
		fmt.Fprintf(os.Stderr, "Connect failed: %v\n", err)
		SendStatus(w, statusDelVolumeErr, "")
		return
	}

	ioctx, err := conn.OpenIOContext(pool)
	if err != nil {
		fmt.Fprintf(os.Stderr, "OpenIOContext failed, pool name:%v, %v\n", pool, err)
		SendStatus(w, statusDelVolumeErr, "")
		return
	}
	defer ioctx.Destroy()

	image := rbd.GetImage(ioctx, volume)
	if err := image.Remove(); err != nil {
		fmt.Fprintf(os.Stderr, "Remove failed %v\n", err)
		SendStatus(w, statusDelVolumeErr, err.Error())
		return
	}

	SendResponse(w, http.StatusOK, http.StatusText(http.StatusOK))
}

/*
GET /?Action=ResizeVolume&PoolName={PoolName}&VolumeName={volumeName}&Size={newSize} HTTP/1.1
Host: xxx.xxx.xxx.xxx
Date: GMT Date
Authorization: (optional)TODO
--------------------------
HTTP /1.1 200 OK
Server: dhcc.ebs
Date: GMT Date

OK
*/
func ResizeVolume(w http.ResponseWriter, r *http.Request) {
	pool := r.FormValue("PoolName")
	volume := r.FormValue("VolumeName")
	size := r.FormValue("Size")
	if pool == "" || volume == "" {
		fmt.Fprintf(os.Stderr, "Invalid Request: %v\n", r.RequestURI)
		SendStatus(w, http.StatusBadRequest, http.StatusText(http.StatusBadRequest))
		return
	}

	newSize, err := strconv.ParseUint(size, 10, 64)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid Request: %v\n", r.RequestURI)
		SendStatus(w, http.StatusBadRequest, http.StatusText(http.StatusBadRequest))
		return
	}

	conn, err := rados.NewConn()
	if err != nil {
		fmt.Fprintf(os.Stderr, "NewConn failed: %v\n", err)
		SendStatus(w, statusResizeVolumeErr, "")
		return
	}
	defer conn.Shutdown()

	if err := conn.ReadDefaultConfigFile(); err != nil {
		fmt.Fprintf(os.Stderr, "ReadDefaultConfigFile failed: %v\n", err)
		SendStatus(w, statusResizeVolumeErr, "")
		return
	}

	if err := conn.Connect(); err != nil {
		fmt.Fprintf(os.Stderr, "Connect failed: %v\n", err)
		SendStatus(w, statusResizeVolumeErr, "")
		return
	}

	ioctx, err := conn.OpenIOContext(pool)
	if err != nil {
		fmt.Fprintf(os.Stderr, "OpenIOContext failed, pool name:%v, %v\n", pool, err)
		SendStatus(w, statusResizeVolumeErr, err.Error())
		return
	}
	defer ioctx.Destroy()

	image := rbd.GetImage(ioctx, volume)
	if err := image.Open(); err != nil {
		fmt.Fprintf(os.Stderr, "image Open failed: %v\n", err)
		SendStatus(w, statusResizeVolumeErr, err.Error())
		return
	}
	defer image.Close()

	if err := image.Resize(newSize); err != nil {
		fmt.Fprintf(os.Stderr, "Resize failed: %v\n", err)
		SendStatus(w, statusResizeVolumeErr, "")
		return
	}

	SendResponse(w, http.StatusOK, http.StatusText(http.StatusOK))
}

/*
GET /?Action=ExportVolume&PoolName={PoolName}&VolumeName={volumeName}&OSSBucket={bucket} HTTP/1.1
Host: xxx.xxx.xxx.xxx
Date: GMT Date
Authorization: (optional)TODO
--------------------------
HTTP /1.1 200 OK
Server: dhcc.ebs
Date: GMT Date

OK
*/
func ExportVolume(w http.ResponseWriter, r *http.Request) {
	/*
	pool := r.FormValue("PoolName")
	volume := r.FormValue("VolumeName")
	bucket := r.FormValue("OSSBucket")
	if pool == "" || volume == "" || bucket == "" {
		fmt.Fprintf(os.Stderr, "Invalid Request: %v\n", r.RequestURI)
		SendStatus(w, http.StatusBadRequest, http.StatusText(http.StatusBadRequest))
		return
	}

	//TODO bucket合法性检查

	conn, err := rados.NewConn()
	if err != nil {
		fmt.Fprintf(os.Stderr, "NewConn failed: %v\n", err)
		SendStatus(w, statusExportVolumeErr, "")
		return
	}
	defer conn.Shutdown()

	if err := conn.ReadDefaultConfigFile(); err != nil {
		fmt.Fprintf(os.Stderr, "ReadDefaultConfigFile failed: %v\n", err)
		SendStatus(w, statusExportVolumeErr, "")
		return
	}

	if err := conn.Connect(); err != nil {
		fmt.Fprintf(os.Stderr, "Connect failed: %v\n", err)
		SendStatus(w, statusExportVolumeErr, "")
		return
	}

	ioctx, err := conn.OpenIOContext(pool)
	if err != nil {
		fmt.Fprintf(os.Stderr, "OpenIOContext failed, pool name:%v, %v\n", pool, err)
		SendStatus(w, statusExportVolumeErr, err.Error())
		return
	}
	defer ioctx.Destroy()

	image := rbd.GetImage(ioctx, volume)
	if err := image.Open(); err != nil {
		fmt.Fprintf(os.Stderr, "image Open failed: %v\n", err)
		SendStatus(w, statusExportVolumeErr, err.Error())
		return
	}
	defer image.Close()

	info, err := image.Stat()
	if err != nil {
		fmt.Fprintf(os.Stderr, "image Stat failed: %v\n", err)
		SendStatus(w, statusExportVolumeErr, err.Error())
		return
	}

	//TODO 暂时导出到文件系统中
	fp, err := os.OpenFile(bucket, os.O_RDONLY | os.O_CREATE | os.O_EXCL, 0644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "OpenFile bucket %v failed: %v\n", bucket, err)
		SendStatus(w, statusExportVolumeErr, err.Error())
		return
	}

	period, err := image.GetStripePeriod()
	if err != nil {
		fmt.Fprintf(os.Stderr, "GetStripePeriod failed: %v\n", err)
		SendStatus(w, statusExportVolumeErr, err.Error())
		return
	}

	fmt.Fprintf(os.Stderr, "GetStripePeriod: %v\n", period)

	ch := make(chan int, MaxProcessorNumber)
	wg := &sync.WaitGroup{}
	for offset := uint64(0); offset < info.Size; offset += period {
		length := uint64(math.Min(float64(period), float64(info.Size - offset)))
		ch <- 1
		wg.Add(1)
		go doRead(fp, offset, length)
	}
	wg.Wait()
*/
	SendResponse(w, http.StatusOK, http.StatusText(http.StatusOK))
}
