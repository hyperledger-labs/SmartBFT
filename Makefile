
.PHONY: protos

all:
	bash ./scripts/build_checks.sh
protos: 
	protoc --go_out=. smartbftprotos/*.proto
	find . -name "*.pb.go" | grep -v "^./vendor" | xargs goimports -w
