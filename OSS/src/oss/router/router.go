package router

import (
	"net/http"
	"oss/clients"
	"oss/log"
	"regexp"
	"strings"
)

var BucketRegexp *regexp.Regexp
var ObjectRegexp *regexp.Regexp

func Init() {
	BucketRegexp = regexp.MustCompile("^/[[:lower:]][[:alnum:]-]*[[:lower:]]$")
	ObjectRegexp = regexp.MustCompile("^/[[:lower:]][[:alnum:]-]*[[:lower:]]/[[:word:]-]+$")

	s3user := clients.S3InitUser("default")

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		switch {
		case matchPutBucketRequest(r):
			s3user.CreateBucket(w, r)
		case matchDeleteBucketRequest(r):
			s3user.DelBucket(w, r)
		case matchListBucketsRequest(r):
			s3user.ListBuckets(w, r)
		case matchListObjectRequest(r):
			s3user.ListObjects(w, r)
		case matchUploadRequest(r):
			s3user.Upload(w, r)
		case matchDelObjectRequest(r):
			s3user.DelObject(w, r)
		case matchDownloadObjectRequest(r):
			s3user.DownloadObject(w, r)
		default:
			clients.SendStatus(w, http.StatusBadRequest)
			osslog.Errorf("Invalid Request: %v %v", r.Method, r.URL)
		}
	})
}

func matchPutBucketRequest(r *http.Request) bool {
	if strings.Compare(r.Method, http.MethodPut) != 0 || BucketRegexp.MatchString(r.RequestURI) != true {
		return false
	}
	return true
}

func matchDeleteBucketRequest(r *http.Request) bool {
	if strings.Compare(r.Method, http.MethodDelete) != 0 || BucketRegexp.MatchString(r.RequestURI) != true {
		return false
	}
	return true
}

func matchListBucketsRequest(r *http.Request) bool {
	if strings.Compare(r.Method, http.MethodGet) != 0 || r.RequestURI != "/" {
		return false
	}
	return true
}

func matchListObjectRequest(r *http.Request) bool {
	if strings.Compare(r.Method, http.MethodGet) != 0 || BucketRegexp.MatchString(r.URL.Path) != true {
		return false
	}
	return true
}

func matchUploadRequest(r *http.Request) bool {
	if strings.Compare(r.Method, http.MethodPut) != 0 || ObjectRegexp.MatchString(r.RequestURI) != true || len(r.Header.Get("x-oss-copy-source")) != 0{
		return false
	}
	return true
}

func matchCopyObjectRequest(r *http.Request) bool {
	if strings.Compare(r.Method, http.MethodPut) != 0 || ObjectRegexp.MatchString(r.RequestURI) != true || len(r.Header.Get("x-oss-copy-source")) == 0{
		return false
	}
	return true
}

func matchDelObjectRequest(r *http.Request) bool {
	if strings.Compare(r.Method, http.MethodDelete) != 0 || ObjectRegexp.MatchString(r.RequestURI) != true {
		return false
	}
	return true
}

func matchDownloadObjectRequest(r *http.Request) bool {
	if strings.Compare(r.Method, http.MethodGet) != 0 || ObjectRegexp.MatchString(r.RequestURI) != true {
		return false
	}
	return true
}
