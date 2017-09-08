package s3util

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

//创建者为当前用户，bucket重名也会创建成功
func S3CreateBucket(svc *s3.S3, bucket string) (*s3.CreateBucketOutput, error) {
	output, err := svc.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucket),
		ACL:    aws.String(s3.BucketCannedACLPublicReadWrite),
	})

	return output, err
}

//删除bucket
func S3DeleteBucket(svc *s3.S3, bucket string) error {
	_, err := svc.DeleteBucket(&s3.DeleteBucketInput{
		Bucket: aws.String(bucket),
	})

	return err
}

//获取所有bucket
func S3ListBucket(svc *s3.S3) (*s3.ListBucketsOutput, error) {
	output, err := svc.ListBuckets(&s3.ListBucketsInput{})
	if err != nil {
		return output, err
	}

	return output, nil
}

//获取bucket位置
func S3GetBucketLocaltion(svc *s3.S3, bucket string) (*s3.GetBucketLocationOutput, error) {
	output, err := svc.GetBucketLocation(&s3.GetBucketLocationInput{
		Bucket: aws.String(bucket),
	})

	return output, err
}

//获取bucket acl
func S3GetBucketAcl(svc *s3.S3, bucket string) (*s3.GetBucketAclOutput, error) {
	output, err := svc.GetBucketAcl(&s3.GetBucketAclInput{
		Bucket: aws.String(bucket),
	})

	return output, err
}

//设置bucket acl
func S3SetBucketAcl(svc *s3.S3, input *s3.PutBucketAclInput) error {
	_, err := svc.PutBucketAcl(input)
	return err
}

//启用/禁用bucket版本
func S3PutBucketVersion(svc *s3.S3, bucket string, status string) error {
	_, err := svc.PutBucketVersioning(&s3.PutBucketVersioningInput{
		Bucket: aws.String(bucket),
		VersioningConfiguration: &s3.VersioningConfiguration{
			Status: aws.String(status),
		},
	})

	return err
}

//获取bucket版本
func S3GetBucketVersion(svc *s3.S3, bucket string) (*s3.GetBucketVersioningOutput, error) {
	output, err := svc.GetBucketVersioning(&s3.GetBucketVersioningInput{
		Bucket: aws.String(bucket),
	})

	return output, err
}
