package processor

import (
	"net/http"
	"fmt"
	"os"
	"strconv"
)

func CreateDisk(w http.ResponseWriter, r *http.Request) {
	poolName := r.FormValue("PoolName")
	volumeName := r.FormValue("VolumeName")
	size := r.FormValue("Size")
	if poolName == "" || volumeName == "" || size == "" {
		fmt.Fprintf(os.Stderr, "Invalid Request: %v\n", r.RequestURI)
		SendStatus(w, http.StatusBadRequest, http.StatusText(http.StatusBadRequest))
		return
	}

	volumeSize, err := strconv.ParseUint(size, 10, 64)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid Request: %v\n", r.RequestURI)
		SendStatus(w, http.StatusBadRequest, http.StatusText(http.StatusBadRequest))
		return
	}

	volume := NewVolume(volumeName, poolName, volumeSize)
	if err := volume.Create(); err != nil {
		fmt.Fprintf(os.Stderr, "Create volume error: %v\n", err)
		SendStatus(w, statusCreateDiskErr, "")
		return
	}

	if err := volume.Map(); err != nil {
		fmt.Fprintf(os.Stderr, "Map volume error: %v\n", err)
		SendStatus(w, statusCreateDiskErr, "")
		return
	}

	if err := volume.UpdateCreateResult(); err != nil {
		fmt.Fprintf(os.Stderr, "Update db error: %v\n", err)
		SendStatus(w, statusCreateDiskErr, "")
		return
	}

	fmt.Fprintf(os.Stderr, "device path: %v\n", volume.devPath)
	SendResponse(w, http.StatusOK, http.StatusText(http.StatusOK))
}

func DelDisk(w http.ResponseWriter, r *http.Request) {

}

func ExtendDisk(w http.ResponseWriter, r *http.Request) {

}

func AttachDisk(w http.ResponseWriter, r *http.Request) {

}

func DetachDisk(w http.ResponseWriter, r *http.Request) {

}

func BackupDisk(w http.ResponseWriter, r *http.Request) {

}
