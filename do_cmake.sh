#!/bin/sh

if test -d build; then
    echo "build dir already exists; rm -rf build and re-run"
    rm -rf build
fi

mkdir build && cd build

cmake ..
make -j20
