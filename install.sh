# build and install binary into go local path
go install github.com/wyfcyx/mdms/client
go install github.com/wyfcyx/mdms/daemon
go install github.com/wyfcyx/mdms/server/dms
go install github.com/wyfcyx/mdms/server/fms
go install github.com/wyfcyx/mdms/server/nms

# create configuration folder

user=$(whoami)
path=/home/$user/.mdms
if [ ! -d $path ]
then
    mkdir ~/.mdms
fi

# copy configuration files into ~/.mdms/
\cp config/init ~/.mdms/init
\cp config/passwd ~/.mdms/passwd
\cp config/group ~/.mdms/group
