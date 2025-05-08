#!/bin/bash
./build-cli.sh
if ! [ -f "PROCESSING" ]; then
    ./build-tileserver-gl.sh
fi
