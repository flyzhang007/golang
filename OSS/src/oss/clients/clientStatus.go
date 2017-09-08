package clients

import "net/http"

const (
	StatusMethodUnmatch         = 700 //method不匹配
	StatusInvalidBucket         = 701 //bucket不合法或缺少bucket字段
	StatusInvalidParam          = 702 //参数不合法，过多或过少等
	StatusCreateBucketError     = 703 //创建bucket失败
	StatusDelBucketError        = 704 //删除bucket失败
	StatusListBucketError       = 705 //列举bucket失败
	StatusTooManyParam          = 706 //请求参数过多
	StatusListObjError          = 707 //列举object失败
	StatusDelObjError           = 708 //删除object失败
	StatusDownloadError         = 709 //下载object失败
)

var statusText = map[int]string{
	StatusMethodUnmatch:        "Method Unmatch.",
	StatusInvalidBucket:        "Invalid Bucket Name.",
	StatusInvalidParam:         "Invalid Parameter",
	StatusCreateBucketError:    "Create Bucket Failed",
	StatusDelBucketError:       "Remove Bucket Failed",
	StatusListBucketError:      "List Bucket Failed",
	StatusTooManyParam:         "Too Many Parameters",
	StatusListObjError:         "List Objects Failed",
	StatusDelObjError:          "Remove Object Failed",
	StatusDownloadError:        "Download Failed",
}

//发送自定义HTTP错误信息
func SendUDFStatus(w http.ResponseWriter, statusCode int) {
	http.Error(w, statusText[statusCode], statusCode)
}

//发送标准HTTP错误信息
func SendStatus(w http.ResponseWriter, statusCode int) {
	http.Error(w, http.StatusText(statusCode), statusCode)
}
