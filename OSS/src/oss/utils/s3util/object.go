package s3util

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"io"
	"strings"
)

//添加一个对象到bucket
func S3PutObject(svc *s3.S3, bucket string, srcObj string, destObj string) (*s3.PutObjectOutput, error) {
	output, err := svc.PutObject(&s3.PutObjectInput{
		Body:   io.ReadSeeker(strings.NewReader(srcObj)),
		Bucket: aws.String(bucket),
		Key:    aws.String(destObj),
	})

	return output, err
}

//从一个bucket中列出N个对象
func S3ListObjects(svc *s3.S3, input *s3.ListObjectsInput) (*s3.ListObjectsOutput, error) {
	output, err := svc.ListObjects(input)

	return output, err
}

//复制一个对象
func S3CopyObject(svc *s3.S3, source string, destBucket string, destObj string) (*s3.CopyObjectOutput, error) {
	output, err := svc.CopyObject(&s3.CopyObjectInput{
		Bucket:     aws.String(destBucket),
		CopySource: aws.String(source),
		Key:        aws.String(destObj),
	})

	return output, err
}

//下载对象
func S3GetObject(svc *s3.S3, input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	output, err := svc.GetObject(input)

	return output, err
}

//获取对象信息
func S3HeadObject(svc *s3.S3, bucket string, object string) (*s3.HeadObjectOutput, error) {
	output, err := svc.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	})

	return output, err
}

//删除一个对象，需具有该bucket的write权限
func S3DeleteObject(svc *s3.S3, bucket string, object string) (*s3.DeleteObjectOutput, error) {
	output, err := svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	})

	return output, err
}

//获取对象ACL
func S3GetObjectAcl(svc *s3.S3, bucket string, object string) (*s3.GetObjectAclOutput, error) {
	output, err := svc.GetObjectAcl(&s3.GetObjectAclInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	})

	return output, err
}

//设置对象ACL
func S3SetObjectAcl(svc *s3.S3, input *s3.PutObjectAclInput) (*s3.PutObjectAclOutput, error) {
	output, err := svc.PutObjectAcl(input)
	return output, err
}

//初始化一个分块上传进程
func S3InitMultipartUpload(svc *s3.S3, bucket string, object string) (*s3.CreateMultipartUploadOutput, error) {
	output, err := svc.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	})
	return output, err
}

//上传分块上传中的其中一个块,upload ID为初始化返回的ID
func S3UploadOnePart(svc *s3.S3, input *s3.UploadPartInput) (*s3.UploadPartOutput, error) {
	output, err := svc.UploadPart(input)
	return output, err
}

//合并上传部分为一个新建对象，从而完成多部分上传
func S3CompleteMultipartUpload(svc *s3.S3, input *s3.CompleteMultipartUploadInput) (*s3.CompleteMultipartUploadOutput, error) {
	output, err := svc.CompleteMultipartUpload(input)
	return output, err
}

//列出分块上传的任务块
func S3ListMultipartUploads(svc *s3.S3, bucket string) (*s3.ListMultipartUploadsOutput, error) {
	output, err := svc.ListMultipartUploads(&s3.ListMultipartUploadsInput{
		Bucket: aws.String(bucket),
	})
	return output, err
}

//取消分块上传
func S3DeleteMultipartUpload(svc *s3.S3, bucket string, object string, uploadId string) (*s3.AbortMultipartUploadOutput, error) {
	output, err := svc.AbortMultipartUpload(&s3.AbortMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(object),
		UploadId: aws.String(uploadId),
	})
	return output, err
}
