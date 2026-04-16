EXTENSION    = $(shell grep -m 1 '"name":' META.json | \
               sed -e 's/[[:space:]]*"name":[[:space:]]*"\([^"]*\)",/\1/')
DISTVERSION  = $(shell grep -m 1 '[[:space:]]\{3\}"version":' META.json | \
               sed -e 's/[[:space:]]*"version":[[:space:]]*"\([^"]*\)",\{0,1\}/\1/')

PG_CONFIG   ?= pg_config
BUILD_DIR   := build
PGXS        := $(shell $(PG_CONFIG) --pgxs)
DLSUFFIX 		:= $(shell make -f "$(PGXS)" show_dl_suffix "$(PGXS)")

all: $(BUILD_DIR)/pg_stat_ch$(DLSUFFIX)

$(BUILD_DIR)/pg_stat_ch$(DLSUFFIX):
	cmake -B $(BUILD_DIR) --preset default
	cmake --build $(BUILD_DIR)

install: $(BUILD_DIR)/pg_stat_ch$(DLSUFFIX)
	cmake --install $(BUILD_DIR) --parallel $$(nproc)

installcheck:
	@echo TODO

dist: $(EXTENSION)-$(DISTVERSION).zip

$(EXTENSION)-$(DISTVERSION).zip:
	git archive --format=zip --prefix="$(EXTENSION)-$(DISTVERSION)/" -o $(EXTENSION)-$(DISTVERSION).zip HEAD
