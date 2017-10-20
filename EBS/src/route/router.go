package route

import (
	"net/http"
	"fmt"
	"os"
	"processor"
)

func Init() {
	http.HandleFunc("/", httpRoute)
}

func httpRoute(w http.ResponseWriter, r *http.Request) {
	if !isValidRequest(r) {
		fmt.Fprintf(os.Stderr, "Method not allowed: %v, expected: %v\n", r.Method, http.MethodGet)
		processor.SendStatus(w, http.StatusMethodNotAllowed, http.StatusText(http.StatusMethodNotAllowed))
		return
	}

	r.ParseForm()
	action := r.FormValue("Action")

	switch {
	case isExportVolume(action):
		processor.ExportVolume(w, r)
	case isCreatePool(action):
		processor.CreatePool(w, r)
	case isInfoPool(action):
		processor.InfoPools(w, r)
	case isInfoPoolIO(action):
		processor.InfoPoolsIO(w, r)
	case isDelPool(action):
		processor.DelPool(w, r)
	case isModPoolReplicaSize(action):
		processor.ModPoolRepSize(w,r )
	case isCreateDisk(action):
		processor.CreateDisk(w, r)
	case isDelDisk(action):
		processor.DelDisk(w, r)
	case isExtendDisk(action):
		processor.ExtendDisk(w, r)
	case isAttachDisk(action):
		processor.AttachDisk(w, r)
	case isDetachDisk(action):
		processor.DetachDisk(w, r)
	case isBackupDisk(action):
		processor.BackupDisk(w, r)
	case isTest(action):
		processor.Test(w, r)
	default:
		fmt.Fprintf(os.Stderr, "Unknown request: %v\n", r.RequestURI)
	}
}

func isTest(action string) bool {
	return action == testAction
}

func isValidRequest(r *http.Request) bool {
	return r.Method == http.MethodGet
}

func isExportVolume(action string) bool {
	return action == exportVolumeAction
}

func isCreatePool(action string) bool {
	return action == createPoolAction
}

func isInfoPool(action string) bool {
	return action == infoPoolAction
}

func isInfoPoolIO(action string) bool {
	return action == infoPoolIOAction
}

func isDelPool(action string) bool {
	return action == delPoolAction
}

func isModPoolReplicaSize(action string) bool {
	return action == modPoolRepSizeAction
}

func isCreateDisk(action string) bool {
	return action == createDiskAction
}

func isDelDisk(action string) bool {
	return action == delDiskAction
}

func isExtendDisk(action string) bool {
	return action == extendDiskAction
}

func isAttachDisk(action string) bool {
	return action == attachDiskAction
}

func isDetachDisk(action string) bool {
	return action == detachDiskAction
}

func isBackupDisk(action string) bool {
	return action == backupDiskAction
}