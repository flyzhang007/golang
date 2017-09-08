set P=%CD%
set GOPATH=%P%
go build -i -o %P%\bin\s3server.exe -gcflags "-N -l" %P%\src\oss\s3server.go
go build -i -o %P%\bin\test.exe -gcflags "-N -l" %P%\src\oss\test.go