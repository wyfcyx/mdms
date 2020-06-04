# build and install binary into go local path
go install github.com/wyfcyx/client
go install github.com/wyfcyx/daemon
go install github.com/wyfcyx/server/dms
go install github.com/wyfcyx/server/fms
go install github.com/wyfcyx/server/nms

# copy configuration files into ~/.mdms/
cp config/init ~/.mdms/init
cp config/passwd ~/.mdms/passwd
cp config/group ~/.mdms/group
