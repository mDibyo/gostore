.PHONY: all clean

PROTOC = protoc
PROTOCFLAGS = --go_out=./

all: protobuf

protobuf: log.proto
	${PROTOC} ${PROTOCFLAGS} log.proto

clean:
	rm -rf *.pb.go

