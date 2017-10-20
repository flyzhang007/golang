export GOROOT=/home/wu_chao/go
export GOBIN=$GOROOT/bin
export PATH=$PATH:$GOBIN
export GOPATH=/home/wu_chao/EBS

export EBSROOT=$GOPATH
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$EBSROOT/src/librados/libs

if [[ -f $EBSROOT/src/librados/libs/librados.so.2.0.0 && ! -f $EBSROOT/src/librados/libs/librados.so ]]; then
    ln -s librados.so.2.0.0 $EBSROOT/src/librados/libs/librados.so
fi

if [[ -f $EBSROOT/src/librados/libs/librbd.so.1.0.0 && ! -f $EBSROOT/src/librados/libs/librbd.so ]]; then
    ln -s librbd.so.1.0.0 $EBSROOT/src/librados/libs/librbd.so
fi
