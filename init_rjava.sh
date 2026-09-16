#!/bin/bash
# Init script: configure JNI headers and install libtirpc for rJava / SqlRender.
set -euo pipefail

export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))

# Install libtirpc-dev (needed by rJava linker) and ensure JDK headers are present
apt-get update -qq
apt-get install -y -qq libtirpc-dev > /dev/null 2>&1

# Reconfigure R's Java settings
R CMD javareconf JAVA_HOME="$JAVA_HOME" 2>/dev/null

echo "init_rjava.sh: javareconf succeeded, libtirpc-dev installed (JAVA_HOME=$JAVA_HOME)"
