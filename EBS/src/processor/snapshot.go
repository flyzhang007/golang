package processor

import (
	"net/http"
	"fmt"
	"os"
	"librados/rados"
	"librados/rbd"
	"encoding/json"
)

/*
GET /?Action=CreateSnapshot&PoolName={PoolName}&VolumeName={volumeName}&Snapshot={name} HTTP/1.1
Host: xxx.xxx.xxx.xxx
Date: GMT Date
Authorization: (optional)TODO
--------------------------
HTTP /1.1 200 OK
Server: dhcc.ebs
Date: GMT Date

OK
*/
func CreateSnapshot(w http.ResponseWriter, r *http.Request) {
	pool := r.FormValue("PoolName")
	volume := r.FormValue("VolumeName")
	snapshot := r.FormValue("Snapshot")
	if pool == "" || volume == "" || snapshot == "" {
		fmt.Fprintf(os.Stderr, "Invalid Request: %v\n", r.RequestURI)
		SendStatus(w, http.StatusBadRequest, http.StatusText(http.StatusBadRequest))
		return
	}

	conn, err := rados.NewConn()
	if err != nil {
		fmt.Fprintf(os.Stderr, "NewConn failed: %v\n", err)
		SendStatus(w, statusCreateSnapshotErr, "")
		return
	}
	defer conn.Shutdown()

	if err := conn.ReadDefaultConfigFile(); err != nil {
		fmt.Fprintf(os.Stderr, "ReadDefaultConfigFile failed: %v\n", err)
		SendStatus(w, statusCreateSnapshotErr, "")
		return
	}

	if err := conn.Connect(); err != nil {
		fmt.Fprintf(os.Stderr, "Connect failed: %v\n", err)
		SendStatus(w, statusCreateSnapshotErr, "")
		return
	}

	ioctx, err := conn.OpenIOContext(pool)
	if err != nil {
		fmt.Fprintf(os.Stderr, "OpenIOContext failed, pool name:%v, %v\n", pool, err)
		SendStatus(w, statusCreateSnapshotErr, err.Error())
		return
	}
	defer ioctx.Destroy()

	image := rbd.GetImage(ioctx, volume)
	if err := image.Open(); err != nil {
		fmt.Fprintf(os.Stderr, "image Open failed: %v\n", err)
		SendStatus(w, statusCreateSnapshotErr, err.Error())
		return
	}
	defer image.Close()

	snapshotInfo, err := image.CreateSnapshot(snapshot)
	if err != nil {
		fmt.Fprintf(os.Stderr, "CreateSnapshot failed: %v\n", err)
		SendStatus(w, statusCreateSnapshotErr, err.Error())
		return
	}
	fmt.Fprintf(os.Stderr, "CreateSnapshot : %v\n", *snapshotInfo)

	SendResponse(w, http.StatusOK, http.StatusText(http.StatusOK))
}

/*
GET /?Action=InfoSnapshot&PoolName={PoolName}&VolumeName={volumeName} HTTP/1.1
Host: xxx.xxx.xxx.xxx
Date: GMT Date
Authorization: (optional)TODO
--------------------------
HTTP /1.1 200 OK
Server: dhcc.ebs
Date: GMT Date

OK
*/
func InfoSnapshot(w http.ResponseWriter, r *http.Request) {
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
		SendStatus(w, statusInfoSnapshotErr, "")
		return
	}
	defer conn.Shutdown()

	if err := conn.ReadDefaultConfigFile(); err != nil {
		fmt.Fprintf(os.Stderr, "ReadDefaultConfigFile failed: %v\n", err)
		SendStatus(w, statusInfoSnapshotErr, "")
		return
	}

	if err := conn.Connect(); err != nil {
		fmt.Fprintf(os.Stderr, "Connect failed: %v\n", err)
		SendStatus(w, statusInfoSnapshotErr, "")
		return
	}

	ioctx, err := conn.OpenIOContext(pool)
	if err != nil {
		fmt.Fprintf(os.Stderr, "OpenIOContext failed, pool name:%v, %v\n", pool, err)
		SendStatus(w, statusInfoSnapshotErr, err.Error())
		return
	}
	defer ioctx.Destroy()

	image := rbd.GetImage(ioctx, volume)
	if err := image.Open(); err != nil {
		fmt.Fprintf(os.Stderr, "image Open failed: %v\n", err)
		SendStatus(w, statusInfoSnapshotErr, err.Error())
		return
	}
	defer image.Close()

	info, err := image.GetSnapshotNames()
	if err != nil {
		fmt.Fprintf(os.Stderr, "GetSnapshotNames failed: %v\n", err)
		SendStatus(w, statusInfoSnapshotErr, err.Error())
		return
	}

	payload, err := json.Marshal(info)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Encode payload failed: %v\n", err)
		SendStatus(w, statusInfoSnapshotErr, "")
		return
	}

	fmt.Fprintf(os.Stderr, "payload: %v\n", string(payload))
	SendResponse(w, http.StatusOK, string(payload))
}

/*
GET /?Action=DelSnapshot&PoolName={PoolName}&VolumeName={volumeName}&Snapshot={name} HTTP/1.1
Host: xxx.xxx.xxx.xxx
Date: GMT Date
Authorization: (optional)TODO
--------------------------
HTTP /1.1 200 OK
Server: dhcc.ebs
Date: GMT Date

OK
*/
func DelSnapshot(w http.ResponseWriter, r *http.Request) {
	pool := r.FormValue("PoolName")
	volume := r.FormValue("VolumeName")
	snapshot := r.FormValue("Snapshot")
	if pool == "" || volume == "" || snapshot == "" {
		fmt.Fprintf(os.Stderr, "Invalid Request: %v\n", r.RequestURI)
		SendStatus(w, http.StatusBadRequest, http.StatusText(http.StatusBadRequest))
		return
	}

	conn, err := rados.NewConn()
	if err != nil {
		fmt.Fprintf(os.Stderr, "NewConn failed: %v\n", err)
		SendStatus(w, statusDelSnapshotErr, "")
		return
	}
	defer conn.Shutdown()

	if err := conn.ReadDefaultConfigFile(); err != nil {
		fmt.Fprintf(os.Stderr, "ReadDefaultConfigFile failed: %v\n", err)
		SendStatus(w, statusDelSnapshotErr, "")
		return
	}

	if err := conn.Connect(); err != nil {
		fmt.Fprintf(os.Stderr, "Connect failed: %v\n", err)
		SendStatus(w, statusDelSnapshotErr, "")
		return
	}

	ioctx, err := conn.OpenIOContext(pool)
	if err != nil {
		fmt.Fprintf(os.Stderr, "OpenIOContext failed, pool name:%v, %v\n", pool, err)
		SendStatus(w, statusDelSnapshotErr, err.Error())
		return
	}
	defer ioctx.Destroy()

	image := rbd.GetImage(ioctx, volume)
	if err := image.Open(); err != nil {
		fmt.Fprintf(os.Stderr, "image Open failed: %v\n", err)
		SendStatus(w, statusDelSnapshotErr, err.Error())
		return
	}
	defer image.Close()

	snapshotCtx := image.GetSnapshot(snapshot)
	if err := snapshotCtx.Remove(); err != nil {
		fmt.Fprintf(os.Stderr, "snapshot Remove failed: %v\n", err)
		SendStatus(w, statusDelSnapshotErr, err.Error())
		return
	}

	SendResponse(w, http.StatusOK, http.StatusText(http.StatusOK))
}