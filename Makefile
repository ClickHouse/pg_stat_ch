EXTENSION    = $(shell grep -m 1 '"name":' META.json | \
               sed -e 's/[[:space:]]*"name":[[:space:]]*"\([^"]*\)",/\1/')
DISTVERSION  = $(shell grep -m 1 '[[:space:]]\{3\}"version":' META.json | \
               sed -e 's/[[:space:]]*"version":[[:space:]]*"\([^"]*\)",\{0,1\}/\1/')

PG_CONFIG   ?= pg_config
CH_CPP_DIR  := third_party/clickhouse-cpp
BUILD_DIR   := build
CH_CPP_LIB  := $(BUILD_DIR)/third_party/libclickhouse-cpp-lib.a
PGXS        := $(shell $(PG_CONFIG) --pgxs)
DLSUFFIX 		:= $(shell make -f "$(PGXS)" show_dl_suffix "$(PGXS)")

all: $(BUILD_DIR)/pg_stat_ch$(DLSUFFIX)

$(CH_CPP_DIR)/CMakeLists.txt:
	git submodule update --init

$(CH_CPP_LIB): $(CH_CPP_DIR)/CMakeLists.txt

$(BUILD_DIR)/pg_stat_ch$(DLSUFFIX): $(CH_CPP_LIB)
	cmake -B $(BUILD_DIR)
	cmake --build $(BUILD_DIR)

install: $(BUILD_DIR)/pg_stat_ch$(DLSUFFIX)
	cmake --install $(BUILD_DIR) --parallel $$(nproc)

installcheck:
	@echo TODO

dist: $(EXTENSION)-$(DISTVERSION).zip

$(EXTENSION)-$(DISTVERSION).zip:
	git archive-all -v --prefix "$(EXTENSION)-$(DISTVERSION)/" --force-submodules $(EXTENSION)-$(DISTVERSION).zip
