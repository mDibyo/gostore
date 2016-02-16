.PHONY: all

PROTOC = protoc
PROTOCFLAGS = --go_out=./

all: pb

pb: log.pb.go
log.pb.go: log.proto 
	${PROTOC} ${PROTOCFLAGS} log.proto

clean:
	rm -f *.pb.go 
