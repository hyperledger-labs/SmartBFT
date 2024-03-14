#!/bin/bash


export GOROOT=$(go env GOROOT)
export GOPATH=$(go env GOPATH)
mkdir -p $GOPATH/src/github.com/hyperledger-labs/
cd ..
echo "[[[[" `pwd` "]]]]"
mv SmartBFT $GOPATH/src/github.com/hyperledger-labs/
cd $GOPATH/src/github.com/hyperledger-labs/SmartBFT
