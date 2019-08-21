#!/usr/bin/env bash -e

git log -1 --no-merges
echo "Testing commit: ------" $(git log -1 --no-merges | head -$(( $(git log -1 --no-merges | wc -l) - 2 )) | tail -1) "------"


go get -u golang.org/x/tools/cmd/goimports
go get -u github.com/golang/protobuf/protoc-gen-go

BUILDDIR=.build
if [ ! -d "$BUILDDIR" ]; then
    mkdir $BUILDDIR
fi
cd $BUILDDIR

PROTOC_ZIP=protoc-3.8.0-linux-x86_64.zip
if [ ! -f "$PROTOC_ZIP" ]; then
    echo "$PROTOC_ZIP does not exist"
    wget https://github.com/protocolbuffers/protobuf/releases/download/v3.8.0/protoc-3.8.0-linux-x86_64.zip
    unzip protoc-3.8.0-linux-x86_64.zip
fi

export PATH=$PATH:$BUILDDIR/bin/

cd ..

unformatted=$(find . -name "*.go" | grep -v "^./vendor" | grep -v "pb.go" | xargs gofmt -l)

if [[ $unformatted == "" ]];then
    echo "gofmt checks passed"
else
    echo "The following files needs gofmt:"
    echo "$unformatted"
    exit 1
fi

unformatted=$(find . -name "*.go" | grep -v "^./vendor" | grep -v "pb.go" | xargs goimports -l)

if [[ $unformatted == "" ]];then
    echo "goimports checks passed"
else
    echo "The following files needs goimports:"
    echo "$unformatted"
    exit 1
fi

make protos
git status | grep "pb.go" | grep -q "modified"
if [ $? -eq 0 ];then
	git status
	echo "protobuf not up to date"
	exit 1
fi

( sleep 60; ps -ef | grep test | grep -v "go test" | grep -v grep | awk '{print $2}' | xargs kill -SIGABRT ) & 

go test -count 1 -race ./... 
if [[ $? -ne 0 ]];then
    echo "unit tests failed"
    exit 1
fi


exit 0
