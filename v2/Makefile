
.PHONY: protos

all:
	bash ./scripts/build_checks.sh
protos: 
	protoc --go_opt=paths=source_relative --go_out=. smartbftprotos/*.proto
	find . -name "*.pb.go" | grep -v "^./vendor" | xargs goimports -w
