CURRENT_DIR := $(CURDIR)
UNAME_S := $(shell uname -s)

LINUX_TYPE ?= linux
DOCKER_COMMAND ?= docker

# Mac-specific settings
ifeq ($(UNAME_S),Darwin)
	DOCKER_COMMAND = finch
endif

.PHONY: all clean

all: clean
	cmake -B build --fresh
	cmake --build build

clean:
	-rm -rf build
	-rm -rf dist

cbuild: clean
	mkdir -p build
	cmake -S $(CURRENT_DIR) -B build -DENABLE_TESTS=ON
	cmake --build build

build: clean
	mkdir -p build
	cmake -S $(CURRENT_DIR) -B build
	cmake --build build

test:
	ctest --test-dir build

java:
	javac -h ./include ../src/main/java/org/opensearch/neuralsearch/sparse/common/NativeLibrary.java


docker: clean
	${DOCKER_COMMAND} rm -f temp_container
	${DOCKER_COMMAND} build --progress=plain -f docker/build/${LINUX_TYPE}/Dockerfile . -t os_ann-linux --build-arg PYTHON_VER=${PY_VER} --build-arg WHL_VER_ARG=${WHL_VER}

docker-run: 
	${DOCKER_COMMAND} build --progress=plain --platform linux/amd64 -f docker/run/Dockerfile . -t os_ann --build-arg PYTHON_VER=${PY_VER} --build-arg WHL_VER_ARG=${WHL_VER}
