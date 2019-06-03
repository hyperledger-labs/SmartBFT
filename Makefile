
.PHONY: protos
protos: 
	protoc --go_out=. protos/*.proto
	find . -name "*.pb.go" | grep -v "^./vendor" | xargs goimports -w
