#!/bin/bash

cd $(dirname $0)
source common.sh
OS=$(get_os)
pushd /root/src/deploy
sudo COMPILE=$1 ./build.sh -h /root/src
popd

cp -r /tmp/publish/${OS} /root/tmp/${OS}
~