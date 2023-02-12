#!/bin/sh

if ! test -d build;then
    echo "please exec do_cmake.sh and make first"
    exit 1
fi

if test -d output;then
    echo "output dir already exists; rm -rf output and re-run"
    rm -rf output
fi

mkdir output
cd build && make install
