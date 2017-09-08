package clients

import (
	"net/http"
	"oss/utils/s3util"
	"oss/log"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"encoding/json"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/aws"
	"strconv"
	"fmt"
	"strings"
	"bytes"
	"io/ioutil"
	"io"
	"oss/conf"
	"errors"
	"math"
	"go/types"
	"math/rand"
	"time"
	"sync"
)

const (
	defaultChunkSizeMB        = 20
	defaultMultiWorkerCnt     = 10
	defaultRecvChunk          = 65535
)

type Chunk struct {
	index      int
	offset     int64
	length     int64
	done       bool
	descriptor string
}

/*
创建bucket
请求：
PUT /{bucket} HTTP/1.1
Host: xxx.xxx.xxx
Date: GMT Date
x-oss-acl: public-read-write
Authorization: {access-key}:{hash-of-header-and-secret}
应答：
HTTP /1.1 200 OK
Server: dhcc.oss
Date: GMT Date
x-oss-errcode: 0
x-oss-errmsg: Success
*/
func (c *RGWconfig) CreateBucket(w http.ResponseWriter, r *http.Request) {
	bucket := r.RequestURI[1:]

	if _, err := s3util.S3CreateBucket(c.s3ctx, bucket); err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			osslog.Errorf("%v\n", aerr.Error())
			http.Error(w, aerr.Code(), StatusCreateBucketError)
		} else {
			osslog.Errorf("Create bucket %v failed: %v\n", bucket, err)
			SendUDFStatus(w, StatusCreateBucketError)
		}
		return
	}

	SendStatus(w, http.StatusOK)
}

/*
删除bucket
请求：
DELETE /{bucket} HTTP/1.1
Host: xxx.xxx.xxx
Date: GMT Date
Authorization: OSS {access-key}:{hash-of-header-and-secret}
应答：
HTTP /1.1 200 OK
Server: dhcc.oss
Date: GMT Date
x-oss-errcode: 0
x-oss-errmsg: Success
*/
func (c *RGWconfig) DelBucket(w http.ResponseWriter, r *http.Request) {
	bucket := r.RequestURI[1:]
	if err := s3util.S3DeleteBucket(c.s3ctx, bucket); err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			osslog.Errorf("%v\n", aerr.Error())
			http.Error(w, aerr.Code(), StatusCreateBucketError)
		} else {
			osslog.Errorf("Delete Bucket %v failed: %v\n", bucket, err)
			SendUDFStatus(w, StatusDelBucketError)
		}
		return
	}

	SendStatus(w, http.StatusOK)
}

/*
获取bucket列表
请求：
GET / HTTP/1.1
Host: xxx.xxx.xxx
Date: GMT Date
应答：
HTTP /1.1 200 OK
Server: dhcc.oss
Date: GMT Date
Content-Length: xxxx
Content-Type: MIME type
x-oss-errcode: 0
x-oss-errmsg: Success

[xxxx bytes data of bucket list]
*/
func (c *RGWconfig) ListBuckets(w http.ResponseWriter, r *http.Request) {
	content, err := s3util.S3ListBucket(c.s3ctx)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			osslog.Errorf("%v\n", aerr.Error())
			http.Error(w, aerr.Code(), StatusListBucketError)
		} else {
			osslog.Errorf("List Buckets failed: %v\n", err)
			SendUDFStatus(w, StatusListBucketError)
		}
	}

	buckets := make([]string, len(content.Buckets))
	for i := 0; i < len(buckets); i++ {
		buckets[i] = aws.StringValue(content.Buckets[i].Name)
	}

	json.NewEncoder(w).Encode(buckets)
}

/*
获取bucket中object列表
请求：
GET /{bucket}?max-keys=15 HTTP/1.1
Host: xxx.xxx.xxx
Date: GMT Date
查询条件
max-keys 最大返回结果数量
prefix   按前缀返回查询结果
应答：
HTTP /1.1 200 OK
Server: dhcc.oss
Date: GMT Date
Content-Length: xxxx
Content-Type: MIME type
x-oss-errcode: 0
x-oss-errmsg: Success

[xxxx bytes data of object list]
 */
func (c *RGWconfig) ListObjects(w http.ResponseWriter, r *http.Request) {
	var input *s3.ListObjectsInput
	bucket := r.URL.Path[1:]

	r.ParseForm()
	if (len(r.Form) != 1) {
		osslog.Errorf("List Objects failed: %v\n", r.URL)
		SendUDFStatus(w, StatusTooManyParam)
		return
	}

	if maxKeysStr := r.FormValue("max-keys"); len(maxKeysStr) != 0 {
		maxKey, err := strconv.ParseInt(maxKeysStr, 10, 64)
		if err != nil {
			osslog.Errorf("List Objects failed: %v\n", r.URL)
			SendUDFStatus(w, StatusInvalidParam)
			return
		}

		input =  &s3.ListObjectsInput{
			Bucket: aws.String(bucket),
			MaxKeys: aws.Int64(maxKey),
		}
	} else if prefix := r.FormValue("prefix"); len(prefix) != 0 {
		input =  &s3.ListObjectsInput{
			Bucket: aws.String(bucket),
			Prefix: aws.String(prefix),
		}
	} else {
		osslog.Errorf("List Objects failed: %v\n", r.URL)
		SendUDFStatus(w, StatusInvalidParam)
		return
	}

	content, err := s3util.S3ListObjects(c.s3ctx, input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			osslog.Errorf("%v\n", aerr.Error())
			http.Error(w, aerr.Code(), StatusListObjError)
		} else {
			osslog.Errorf("List Objects failed: %v\n", err)
			SendUDFStatus(w, StatusListObjError)
		}
		return
	}

	objs := make([]string, len(content.Contents))
	for i := 0; i < len(objs); i++ {
		objs[i] = aws.StringValue(content.Contents[i].Key)
	}

	json.NewEncoder(w).Encode(objs)
	return
}

/*
设置bucket acl
请求：
PUT /{bucket}?acl HTTP/1.1
Host: xxx.xxx.xxx
Date: GMT Date
x-oss-acl: e.g. private,public-read,public-read-write,authenticated-read
Authorization: OSS {access-key}:{hash-of-header-and-secret}
应答：
HTTP /1.1 200 OK
Server: dhcc.oss
Date: GMT Date
x-oss-errcode: 0
x-oss-errmsg: Success
*/
func (c *RGWconfig) SetBucketAcl(w http.ResponseWriter, r *http.Request) {

}

/*
上传对象
请求：
PUT /{bucket}/{object} HTTP/1.1
Host: xxx.xxx.xxx
Date: GMT Date
Content-Length: content length
Content-Type: MIME type
Content-MD5: content md5
x-oss-acl: ACL policy, e.g. private,public-read,public-read-write,authenticated-read
Authorization: OSS {access-key}:{hash-of-header-and-secret}
应答：
HTTP /1.1 200 OK
Server: dhcc.oss
Date: GMT Date
x-oss-errcode: 0
x-oss-errmsg: Success
*/
func (c *RGWconfig) Upload(w http.ResponseWriter, r *http.Request) {
	fmt.Println("=====upload=====")
	arr := strings.Split(r.RequestURI[1:], "/")
	bucket := arr[0]
	destObj := arr[1]
	fmt.Println(bucket)
	fmt.Println(destObj)
	//s3util.S3PutObject(c.s3ctx, bucket, "", destObj)
	SendStatus(w, http.StatusOK)
}

/*
删除对象
请求：
DELETE /{bucket}/{object} HTTP/1.1
Host: xxx.xxx.xxx
Date: GMT Date
Authorization: OSS {access-key}:{hash-of-header-and-secret}
应答：
HTTP /1.1 200 OK
Server: dhcc.oss
Date: GMT Date
x-oss-errcode: 0
x-oss-errmsg: Success
*/
func (c *RGWconfig) DelObject(w http.ResponseWriter, r *http.Request) {
	fmt.Println("=====delete=====")
	arr := strings.Split(r.RequestURI[1:], "/")
	bucket := arr[0]
	destObj := arr[1]
	fmt.Println(bucket)
	fmt.Println(destObj)
	_, err := s3util.S3DeleteObject(c.s3ctx, bucket, destObj)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			osslog.Errorf("%v\n", aerr.Error())
			http.Error(w, aerr.Code(), StatusDelObjError)
		} else {
			osslog.Errorf("Delete Objects %v failed: %v\n", r.RequestURI, err)
			SendUDFStatus(w, StatusDelObjError)
		}
		return
	}

	SendStatus(w, http.StatusOK)
}

/*
下载对象
请求：
GET /{bucket}/{object} HTTP/1.1
Host: xxx.xxx.xxx
Date: GMT Date
Range: bytes=beginbyte-endbyte(optional)
Authorization: OSS {access-key}:{hash-of-header-and-secret}
应答：
HTTP /1.1 200 OK
Server: dhcc.oss
Date: GMT Date
Content-Length: xxxx
Content-Type: MIME type
Content-Range: bytes=beginbyte-endbyte(optional)
x-oss-errcode: 0
x-oss-errmsg: Success

[xxxx bytes of object data]
*/
func (c *RGWconfig) DownloadObject(w http.ResponseWriter, r *http.Request) {
	fmt.Println("=====delete=====")
	chunks, err := preDownload(c, r)
	if err != nil {
		osslog.Errorf("%v\n", err.Error())
		SendUDFStatus(w, StatusDownloadError)
		return
	}

	download(chunks, c, r)

}

/*
设置object acl
请求：
PUT /{bucket}/{object}?acl HTTP/1.1
Host: xxx.xxx.xxx
Date: GMT Date
x-oss-acl: e.g. private,public-read,public-read-write,authenticated-read
Authorization: OSS {access-key}:{hash-of-header-and-secret}
应答：
HTTP /1.1 200 OK
Server: dhcc.oss
Date: GMT Date
x-oss-errcode: 0
x-oss-errmsg: Success
*/
func (c *RGWconfig) SetObjectAcl(w http.ResponseWriter, r *http.Request) {

}

/*
object分块上传
请求：
POST /{bucket}/{object}?uploads HTTP/1.1
Host: xxx.xxx.xxx
Date: GMT Date
Content-Length: content length
Content-Type: MIME type
Content-MD5: content md5
x-oss-acl: e.g. private,public-read,public-read-write,authenticated-read
Authorization: OSS {access-key}:{hash-of-header-and-secret}
应答：
HTTP /1.1 200 OK
Server: dhcc.oss
Date: GMT Date
Location: URI of new object
ETag: Entity tag of the object
x-oss-errcode: 0
x-oss-errmsg: Success
*/
func (c *RGWconfig) UploadMultipart(w http.ResponseWriter, r *http.Request) {

}

func preDownload(c *RGWconfig, r *http.Request) ([]Chunk, error){
	arr := strings.Split(r.RequestURI[1:], "/")
	bucket := arr[0]
	destObj := arr[1]

	info, herr := s3util.S3HeadObject(c.s3ctx, bucket, destObj)
	if herr != nil {
		return nil, herr
	}
	objectSize := aws.Int64Value(info.ContentLength)
	chunkSize, cerr := conf.GetInt("global", "multipart_chunk_size_mb")
	if !cerr {
		chunkSize = defaultChunkSizeMB
	}
	chunkCnt := int(math.Ceil(float64(objectSize) / float64(chunkSize)))
	chunks := make([]Chunk, chunkCnt)
	for i := 0; i < chunkCnt; i++ {
		chunks[i].index = i
		chunks[i].offset = int64(i * chunkSize * 1024 * 1024)
		left := objectSize - int64((i + 1) * chunkSize * 1024 * 1024)
		if left < 0 {
			chunks[i].length = left
		} else {
			chunks[i].length = int64(chunkSize * 1024 * 1024)
		}
		chunks[i].done = false
		chunks[i].descriptor = nil
	}

	fmt.Println(bucket)
	fmt.Println(destObj)

	return chunks, nil
}

func download(chunks []Chunk, c *RGWconfig, r *http.Request) {
	threadCnt, err := conf.GetInt("global", "multipart_max_workers")
	if !err {
		threadCnt = defaultMultiWorkerCnt
	}

	if len(chunks) < threadCnt {
		threadCnt = len(chunks)
	}

	ch := make(chan int, threadCnt)
	wg := &sync.WaitGroup{}
	for i := 0; i < len(chunks); i++ {
		ch <- 1
		wg.Add(1)
		go doWork(chunks[i], c, r)
	}
	wg.Wait()
}

func doWork(chunk Chunk, c *RGWconfig,  r *http.Request) {
	arr := strings.Split(r.RequestURI[1:], "/")
	bucket := arr[0]
	destObj := arr[1]

	resp, err := s3util.S3GetObject(c.s3ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key: aws.String(destObj),
		Range: aws.String(fmt.Sprintf("bytes=%d-%d", chunk.offset, chunk.offset + chunk.offset - 1)),
	})
	defer resp.Body.Close()
	if err != nil {
		osslog.Errorf("object:%v/%v, chunkId:%v, offset:%v, len:%v, %v\n", bucket, destObj, chunk.index, chunk.offset, chunk.length, err.Error())
		chunk.done = false
		chunk.descriptor = err.Error()
		return
	}

	recvChunk, cerr := conf.GetInt("global", "recv_chunk")
	if !cerr {
		recvChunk = defaultRecvChunk
	}
	recvBuf := make([]byte, recvChunk)
	ntotal := int64(0)
	for {
		nbytes, err := resp.Body.Read(recvBuf[0:])
		ntotal += int64(nbytes)
		if (err != nil && err != io.EOF) || nbytes == 0 {
			break
		}
	}
}