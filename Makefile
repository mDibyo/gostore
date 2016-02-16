.PHONY: all pb clean

PB_DIR = ./pb

GO = go
GO_SOURCES = storemap.go

all: pb go

pb:
	$(MAKE) -C ${PB_DIR} pb

go:
	${GO} fmt .
	${GO} install .

clean:
	$(MAKE)	-C ${PB_DIR} clean

