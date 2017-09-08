package clients

import (
	"net/http"
)

/*
web表单上传object
请求：
POST /{bucket}/{object}?form HTTP/1.1
Host: xxx.xxx.xxx
Date: GMT Date
Content-Type: MIME type
Content-Length: content length

Authorization: OSS {access-key}:{hash-of-header-and-secret}

----WebKitFormBoundaryE19zNvXGzXaLvS5C
Content-Disposition: form-data; name="pic"; filename="1.png"
Content-Type: image/png

[object data]

----WebKitFormBoundaryE19zNvXGzXaLvS5C
应答：
HTTP /1.1 200 OK
Server: dhcc.oss
Date: GMT Date
Location: URI of new object
ETag: Entity tag of the object(optional)
x-oss-errcode: 0
x-oss-errmsg: Success
*/
func UDFFormUpload(w http.ResponseWriter, r *http.Request) {

}
