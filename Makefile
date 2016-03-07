.PHONY: all pb cmd clean

CMD_DIR = ./cmd
PB_DIR = ./pb

GO = go
GO_SOURCES = storemap.go

all: pb go cmd

pb:
	$(MAKE) -C ${PB_DIR} pb

go: pb
	${GO} fmt .
	${GO} install .

cmd:
	$(MAKE) -C ${CMD_DIR} all

test:
	${GO} test -v

clean:
	$(MAKE)	-C ${PB_DIR} clean
	$(MAKE) -C ${CMD_DIR} clean
