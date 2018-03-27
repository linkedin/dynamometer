#!/usr/bin/env bash
# Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

if [ "$#" != 1 ] || [ "$1" == "-h" ] || [ "$1" == "--help" ]; then
  echo "Usage: ./create-slim-hadoop-tar.sh path-to-hadoop-tar"
  echo "  Takes path-to-hadoop-tar as a hadoop.tar.gz binary distribution"
  echo "  and removes portions of it that are unnecessary for dynamometer"
  echo "  (e.g. unrelated components like YARN)."
  echo "  This overwrites the original file."
  echo "  This is idempotent; you can safely rerun it on the same tar."
  exit 1
fi

hadoopTar="$1"

echo "Slimming $hadoopTar; size before is `ls -lh "$hadoopTar" | awk '{ print $5 }'`"

hadoopTarTmp="$hadoopTar.temporary"

mkdir -p "$hadoopTarTmp"

tar xzf "$hadoopTar" -C "$hadoopTarTmp"
baseDir="$hadoopTarTmp/`ls -1 "$hadoopTarTmp" | head -n 1`" # Should only be one subdir
hadoopShare="$baseDir/share/hadoop"

# Remove unnecessary files
rm -rf ${baseDir}/share/doc ${hadoopShare}/mapreduce ${hadoopShare}/yarn \
       ${hadoopShare}/kms ${hadoopShare}/tools ${hadoopShare}/httpfs \
       ${hadoopShare}/*/sources ${hadoopShare}/*/jdiff

tar czf "$hadoopTarTmp.tar.gz" -C "$hadoopTarTmp" .
rm -rf "$hadoopTarTmp"
mv -f "$hadoopTarTmp.tar.gz" "$hadoopTar"

echo "Finished; size after is `ls -lh "$hadoopTar" | awk '{ print $5 }'`"