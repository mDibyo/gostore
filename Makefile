.PHONY: all clean

PROTOC = protoc
PROTOCFLAGS = --go_out=./

GO = go

all: go

protobuf: log.proto
	${PROTOC} ${PROTOCFLAGS} log.proto

go: storemap.go protobuf
	${GO} fmt .
	${GO} install .

clean:
	rm -rf *.pb.go

