package processor

import (
	"net/http"
	"librados/rados"
	"fmt"
	"os"
	"encoding/json"
	"db"
)

type PoolSummarize struct {
	Replica_num     uint64
	Objects_num     uint64
	Used_bytes      uint64
}

type PoolIOSummarize struct {
	IO_read         uint64
	IO_write        uint64
	IO_read_kb      uint64
	IO_write_kb     uint64
}

func Test(w http.ResponseWriter, r *http.Request) {
	handle := db.GetDBHandler()
	rows, _ := handle.Query("SELECT * FROM test")
	name := ""
	age := 0
	for rows.Next() {
		rows.Scan(&name, &age)
		fmt.Println(name, age)
	}
	SendResponse(w, http.StatusOK, http.StatusText(http.StatusOK))
}

func NewConnAndOpenPool(pool string) (*rados.Conn, *rados.IOContext, error) {
	conn, err := rados.NewConn()
	if err != nil {
		fmt.Fprintf(os.Stderr, "NewConn failed: %v\n", err)
		return nil, nil, err
	}

	if err := conn.ReadDefaultConfigFile(); err != nil {
		fmt.Fprintf(os.Stderr, "ReadDefaultConfigFile failed: %v\n", err)
		return conn, nil, err
	}

	if err := conn.Connect(); err != nil {
		fmt.Fprintf(os.Stderr, "Connect failed: %v\n", err)
		return conn, nil, err
	}

	ioctx, err := conn.OpenIOContext(pool)
	if err != nil {
		fmt.Fprintf(os.Stderr, "OpenIOContext failed: %v\n", err)
		return conn, nil, err
	}

	return conn, ioctx, nil
}

func DisConnAndClosePool(conn *rados.Conn, context *rados.IOContext) {
	if context != nil {
		context.Destroy()
	}

	if conn != nil {
		conn.Shutdown()
	}
}

/*
GET /?Action=CreatePool&PoolName={PoolName|*} HTTP/1.1
Host: xxx.xxx.xxx.xxx
Date: GMT Date
Authorization: (optional)TODO
--------------------------
HTTP /1.1 200 OK
Server: dhcc.ebs
Date: GMT Date

OK
*/
func CreatePool(w http.ResponseWriter, r *http.Request) {
	pool := r.FormValue("PoolName")
	if pool == "" {
		fmt.Fprintf(os.Stderr, "Invalid Request: %v\n", r.RequestURI)
		SendStatus(w, http.StatusBadRequest, http.StatusText(http.StatusBadRequest))
		return
	}

	conn, err := rados.NewConn()
	if err != nil {
		fmt.Fprintf(os.Stderr, "NewConn failed: %v\n", err)
		SendStatus(w, statusCreatePoolErr, "")
		return
	}
	defer conn.Shutdown()

	if err := conn.ReadDefaultConfigFile(); err != nil {
		fmt.Fprintf(os.Stderr, "ReadDefaultConfigFile failed: %v\n", err)
		SendStatus(w, statusCreatePoolErr, "")
		return
	}

	if err := conn.Connect(); err != nil {
		fmt.Fprintf(os.Stderr, "Connect failed: %v\n", err)
		SendStatus(w, statusCreatePoolErr, "")
		return
	}

	if err := conn.MakePool(pool); err != nil {
		fmt.Fprintf(os.Stderr, "MakePool failed, pool name:%v, %v\n", pool, err)
		SendStatus(w, statusCreatePoolErr, err.Error())
		return
	}

	SendResponse(w, http.StatusOK, http.StatusText(http.StatusOK))
}

/*
GET /?Action=InfoPool&PoolName={PoolName|*} HTTP/1.1
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
func InfoPools(w http.ResponseWriter, r *http.Request) {
	pool := r.FormValue("PoolName")
	if pool == "" {
		fmt.Fprintf(os.Stderr, "Invalid Request: %v\n", r.RequestURI)
		SendStatus(w, http.StatusBadRequest, http.StatusText(http.StatusBadRequest))
		return
	}

	conn, err := rados.NewConn()
	if err != nil {
		fmt.Fprintf(os.Stderr, "NewConn failed: %v\n", err)
		SendStatus(w, statusInfoPoolErr, "")
		return
	}
	defer conn.Shutdown()

	if err := conn.ReadDefaultConfigFile(); err != nil {
		fmt.Fprintf(os.Stderr, "ReadDefaultConfigFile failed: %v\n", err)
		SendStatus(w, statusInfoPoolErr, "")
		return
	}

	if err := conn.Connect(); err != nil {
		fmt.Fprintf(os.Stderr, "Connect failed: %v\n", err)
		SendStatus(w, statusInfoPoolErr, "")
		return
	}

	var pools []string
	if pool == "*" {
		if pools, err = conn.ListPools(); err != nil {
			fmt.Fprintf(os.Stderr, "ListPools failed: %v\n", err)
			SendStatus(w, statusInfoPoolErr, "")
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

	poolsInfo := make(map[string]PoolSummarize)
	for i := 0; i < len(pools); i++ {
		ioctx, err := conn.OpenIOContext(pools[i])
		if err != nil {
			fmt.Fprintf(os.Stderr, "OpenIOContext failed, pool name:%v, %v\n", pools[i], err)
			SendStatus(w, statusInfoPoolErr, "")
			return
		}
		defer ioctx.Destroy()

		PoolStat, err := ioctx.GetPoolStats()
		if err != nil {
			fmt.Fprintf(os.Stderr, "GetPoolStats failed, pool name:%v, %v\n", pools[i], err)
			SendStatus(w, statusInfoPoolErr, "")
			return
		}

		poolsInfo[pools[i]] = PoolSummarize{
			Replica_num: PoolStat.Num_object_copies / PoolStat.Num_objects,
			Used_bytes: PoolStat.Num_bytes,
			Objects_num: PoolStat.Num_objects,
		}
	}

	payload, err := json.Marshal(poolsInfo)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Encode payload failed: %v\n", err)
		SendStatus(w, statusInfoPoolErr, "")
		return
	}

	SendResponse(w, http.StatusOK, string(payload))
}

/*
GET /?Action=InfoPoolsIO&PoolName={PoolName|*} HTTP/1.1
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
//TODO 读写时的IO
func InfoPoolsIO(w http.ResponseWriter, r *http.Request) {
	pool := r.FormValue("PoolName")
	if pool == "" {
		fmt.Fprintf(os.Stderr, "Invalid Request: %v\n", r.RequestURI)
		SendStatus(w, http.StatusBadRequest, http.StatusText(http.StatusBadRequest))
		return
	}

	conn, err := rados.NewConn()
	if err != nil {
		fmt.Fprintf(os.Stderr, "NewConn failed: %v\n", err)
		SendStatus(w, statusInfoPoolIOErr, "")
		return
	}
	defer conn.Shutdown()

	if err := conn.ReadDefaultConfigFile(); err != nil {
		fmt.Fprintf(os.Stderr, "ReadDefaultConfigFile failed: %v\n", err)
		SendStatus(w, statusInfoPoolIOErr, "")
		return
	}

	if err := conn.Connect(); err != nil {
		fmt.Fprintf(os.Stderr, "Connect failed: %v\n", err)
		SendStatus(w, statusInfoPoolIOErr, "")
		return
	}

	var pools []string
	if pool == "*" {
		if pools, err = conn.ListPools(); err != nil {
			fmt.Fprintf(os.Stderr, "ListPools failed: %v\n", err)
			SendStatus(w, statusInfoPoolIOErr, "")
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

	poolsIOInfo := make(map[string]PoolIOSummarize)
	for i := 0; i < len(pools); i++ {
		ioctx, err := conn.OpenIOContext(pools[i])
		if err != nil {
			fmt.Fprintf(os.Stderr, "OpenIOContext failed, pool name:%v, %v\n", pools[i], err)
			SendStatus(w, statusInfoPoolIOErr, "")
			return
		}
		defer ioctx.Destroy()

		PoolStat, err := ioctx.GetPoolStats()
		if err != nil {
			fmt.Fprintf(os.Stderr, "GetPoolStats failed, pool name:%v, %v\n", pools[i], err)
			SendStatus(w, statusInfoPoolIOErr, "")
			return
		}

		poolsIOInfo[pools[i]] = PoolIOSummarize{
			IO_read: PoolStat.Num_rd,
			IO_read_kb: PoolStat.Num_rd_kb,
			IO_write: PoolStat.Num_wr,
			IO_write_kb: PoolStat.Num_wr_kb,
		}
	}

	payload, err := json.Marshal(poolsIOInfo)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Encode payload failed: %v\n", err)
		SendStatus(w, statusInfoPoolIOErr, "")
		return
	}

	SendResponse(w, http.StatusOK, string(payload))
}

/*
GET /?Action=DelPool&PoolName={PoolName} HTTP/1.1
Host: xxx.xxx.xxx.xxx
Date: GMT Date
Authorization: (optional)TODO
--------------------------
HTTP /1.1 200 OK
Server: dhcc.ebs
Date: GMT Date

OK
*/
func DelPool(w http.ResponseWriter, r *http.Request) {
	pool := r.FormValue("PoolName")
	if pool == "" {
		fmt.Fprintf(os.Stderr, "Invalid Request: %v\n", r.RequestURI)
		SendStatus(w, http.StatusBadRequest, http.StatusText(http.StatusBadRequest))
		return
	}

	conn, err := rados.NewConn()
	if err != nil {
		fmt.Fprintf(os.Stderr, "NewConn failed: %v\n", err)
		SendStatus(w, statusDelPoolErr, "")
		return
	}
	defer conn.Shutdown()

	if err := conn.ReadDefaultConfigFile(); err != nil {
		fmt.Fprintf(os.Stderr, "ReadDefaultConfigFile failed: %v\n", err)
		SendStatus(w, statusDelPoolErr, "")
		return
	}

	if err := conn.Connect(); err != nil {
		fmt.Fprintf(os.Stderr, "Connect failed: %v\n", err)
		SendStatus(w, statusDelPoolErr, "")
		return
	}

	if err := conn.DeletePool(pool); err != nil {
		fmt.Fprintf(os.Stderr, "DeletePool failed, pool name:%v, %v\n", pool, err)
		SendStatus(w, statusDelPoolErr, err.Error())
		return
	}

	SendResponse(w, http.StatusOK, http.StatusText(http.StatusOK))
}

/*
GET /?Action=ModPoolRepSize&PoolName={PoolName}&Size={replicaSize} HTTP/1.1
Host: xxx.xxx.xxx.xxx
Date: GMT Date
Authorization: (optional)TODO
--------------------------
HTTP /1.1 200 OK
Server: dhcc.ebs
Date: GMT Date

OK
*/
func ModPoolRepSize(w http.ResponseWriter, r *http.Request) {
	pool := r.FormValue("PoolName")
	if pool == "" {
		fmt.Fprintf(os.Stderr, "Invalid Request: %v\n", r.RequestURI)
		SendStatus(w, http.StatusBadRequest, http.StatusText(http.StatusBadRequest))
		return
	}

	conn, err := rados.NewConn()
	if err != nil {
		fmt.Fprintf(os.Stderr, "NewConn failed: %v\n", err)
		SendStatus(w, statusModPoolErr, "")
		return
	}
	defer conn.Shutdown()

	if err := conn.ReadDefaultConfigFile(); err != nil {
		fmt.Fprintf(os.Stderr, "ReadDefaultConfigFile failed: %v\n", err)
		SendStatus(w, statusModPoolErr, "")
		return
	}

	if err := conn.Connect(); err != nil {
		fmt.Fprintf(os.Stderr, "Connect failed: %v\n", err)
		SendStatus(w, statusModPoolErr, "")
		return
	}

	size := r.FormValue("Size")
	var cmd = map[string]string{
		"prefix" : "osd pool set",
		"pool" : pool,
		"var" : "size",
		"val" : size,
	}
	payload, err := json.Marshal(cmd)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Encode command failed: %v\n", err)
		SendStatus(w, statusModPoolErr, "")
		return
	}

	content, info, err := conn.MonCommand(payload)
	if err != nil {
		fmt.Fprintf(os.Stderr, "MonCommand failed, cmd:%v, %v\n", string(payload), err)
		SendStatus(w, statusModPoolErr, err.Error())
		return
	}
	fmt.Fprintf(os.Stderr, "content:[%v]\ninfo:[%v]\n", content, info)

	SendResponse(w, http.StatusOK, http.StatusText(http.StatusOK))
}