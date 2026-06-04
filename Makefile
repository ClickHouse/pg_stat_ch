EXTENSION    = $(shell grep -m 1 '"name":' META.json | \
               sed -e 's/[[:space:]]*"name":[[:space:]]*"\([^"]*\)",/\1/')
DISTVERSION  = $(shell grep -m 1 '[[:space:]]\{3\}"version":' META.json | \
               sed -e 's/[[:space:]]*"version":[[:space:]]*"\([^"]*\)",\{0,1\}/\1/')

PG_CONFIG   ?= pg_config
BUILD_DIR   := build
CMAKE       ?= cmake
CMAKE_ARGS  ?= -DPG_CONFIG=$(PG_CONFIG)

.PHONY: all install installcheck dist

all: third_party/clickhouse-c/clickhouse.h
	$(CMAKE) -B $(BUILD_DIR) $(CMAKE_ARGS)
	$(CMAKE) --build $(BUILD_DIR)

third_party/clickhouse-c/clickhouse.h:
	git submodule update --init third_party/clickhouse-c

install: all
	$(CMAKE) --install $(BUILD_DIR) --parallel $$(nproc)

installcheck:
	@echo TODO

dist: $(EXTENSION)-$(DISTVERSION).zip

$(EXTENSION)-$(DISTVERSION).zip:
	git archive-all -v --prefix "$(EXTENSION)-$(DISTVERSION)/" --force-submodules $(EXTENSION)-$(DISTVERSION).zip
