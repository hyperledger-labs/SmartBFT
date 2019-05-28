
.PHONY: protos
protos: 
	protoc --go_out=. protos/*.proto
