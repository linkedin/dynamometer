#!/usr/bin/env bash
# Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

# This script simply passes its arguments along to the block generation
# driver after finding a hadoop command in PATH/HADOOP_COMMON_HOME/HADOOP_HOME
# (searching in that order).

if type hadoop &> /dev/null; then
  hadoop_cmd="hadoop"
elif type "$HADOOP_COMMON_HOME/bin/hadoop" &> /dev/null; then
  hadoop_cmd="$HADOOP_COMMON_HOME/bin/hadoop"
elif type "$HADOOP_HOME/bin/hadoop" &> /dev/null; then
  hadoop_cmd="$HADOOP_HOME/bin/hadoop"
else
  echo "Unable to find a valid hadoop command to execute; exiting."
  exit 1
fi

script_pwd="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/.."

for f in ${script_pwd}/lib/*.jar; do
  # Skip adding the blockgen JAR since it is added by the `hadoop jar` command
  if [[ "$f" != *"dynamometer-blockgen-"* ]]; then
    export HADOOP_CLASSPATH="$HADOOP_CLASSPATH:$f"
  fi
done
"$hadoop_cmd" jar ${script_pwd}/lib/dynamometer-blockgen-*.jar \
  com.linkedin.dynamometer.blockgenerator.GenerateBlockImagesDriver "$@"
