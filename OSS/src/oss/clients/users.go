package clients

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"oss/conf"
	"oss/log"
)

type RGWconfig struct {
	uid       string
	endpoint  string
	accesskey string
	secretkey string
	s3ctx     *s3.S3
}

func S3InitUser(uid string) *RGWconfig {
	var s3user *RGWconfig
	if conf, isexist := conf.GetSectionConf(uid); isexist {
		s3user = &RGWconfig{
			uid:         uid,
			endpoint:    conf["endpoint"],
			accesskey:   conf["access_key"],
			secretkey:   conf["secret_key"],
			s3ctx:       nil,
		}
	} else {
		osslog.Fatalf("Unknown User: %v\n", uid)
	}

	err := S3Connection(s3user)
	if err != nil {
		osslog.Errorf("New session failed, due to: %v\n", err.Error())
		return nil
	}

	return s3user
}

func S3Connection(userconf *RGWconfig) error {
	creds := credentials.NewStaticCredentials(userconf.accesskey, userconf.secretkey, "")
	sess, err := session.NewSession(&aws.Config{
		Endpoint:                      aws.String(userconf.endpoint),
		DisableSSL:                    aws.Bool(true),
		Region:                        aws.String(s3.BucketLocationConstraintCnNorth1),
		S3ForcePathStyle:              aws.Bool(true),
		Credentials:                   creds,
		CredentialsChainVerboseErrors: aws.Bool(true),
	})
	if err != nil {
		return err
	}

	svc := s3.New(sess)
	userconf.s3ctx = svc

	return nil
}