#! /bin/bash -l

export LD_LIBRARY_PATH=/opt/lib:$LD_LIBRARY_PATH
export PKG_CONFIG_PATH=/opt/lib/pkgconfig:$PKG_CONFIG_PATH
export EXTERNAL_IP=$(curl ipinfo.io/ip)

xvfb-run --server-args="-screen 0 1024x768x24" tileserver-gl -p 8080 --public_url http://${EXTERNAL_IP}/tiles/ --config build-cli/tileserver/config.json