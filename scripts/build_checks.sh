#!/usr/bin/env bash

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


exit 0
