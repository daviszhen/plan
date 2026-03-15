# Generate Go code from Lance proto files.
# Requires: protoc, protoc-gen-go (go install google.golang.org/protobuf/cmd/protoc-gen-go@latest)
# Optional: set PROTOC_INCLUDE to point to directory containing google/protobuf/*.proto (e.g. /usr/include)

PROTO_DIR := .
GO_OUT := .
PROTOC_INCLUDE ?= /usr/include

.PHONY: all clean

all: file.pb.go table.pb.go transaction.pb.go

file.pb.go: file.proto
	protoc -I $(PROTO_DIR) -I $(PROTOC_INCLUDE) --go_out=$(GO_OUT) --go_opt=paths=source_relative $(PROTO_DIR)/file.proto

table.pb.go: table.proto file.pb.go
	protoc -I $(PROTO_DIR) -I $(PROTOC_INCLUDE) --go_out=$(GO_OUT) --go_opt=paths=source_relative $(PROTO_DIR)/table.proto

transaction.pb.go: transaction.proto table.pb.go
	protoc -I $(PROTO_DIR) -I $(PROTOC_INCLUDE) --go_out=$(GO_OUT) --go_opt=paths=source_relative $(PROTO_DIR)/transaction.proto

clean:
	rm -f file.pb.go table.pb.go transaction.pb.go
