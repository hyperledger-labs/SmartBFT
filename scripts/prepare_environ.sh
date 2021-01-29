#!/bin/bash

echo "Installing Dep"
sudo apt-get install go-dep


export GOROOT=$(go env GOROOT)
export GOPATH=$(go env GOPATH)
mkdir -p $GOPATH/src/github.com/SmartBFT-Go/
cd ..
echo "[[[[" `pwd` "]]]]"
mv consensus $GOPATH/src/github.com/SmartBFT-Go/
cd $GOPATH/src/github.com/SmartBFT-Go/consensus


echo "Installing dependencies in " `pwd`
dep ensure

