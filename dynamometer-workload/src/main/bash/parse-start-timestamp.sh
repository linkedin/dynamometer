#!/usr/bin/env bash
# Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

# This script will determine the timestamp of the last transaction appearing in a
# given fsimage by looking at the corresponding edits file. This is useful to determine
# from whence to start collecting audit logs to replay against the fsimage.

if [ $# -lt 1 ] || [ "$1" == "-h" ] || [ "$1" == "--help" ]; then
  echo "Usage: ./parse-start-timestamp.sh image-txid [ edits-dir ]"
  echo "       Finds the last timestamp present in the edit file which ends in"
  echo "       the specified transaction ID (leading 0s not required)."
  echo "       If edits-dir is specified, looks for edit files under"
  echo "       edits-dir/current. Otherwise, looks in the current directory."
  exit 1
fi
if [ `command -v gawk` == "" ]; then
  echo "This script requires gawk to be available."
  exit 1
fi
image_txid="$1"
if [ $# -ge 2 ]; then
  edits_dir="$2/current"
else
  edits_dir="`pwd`"
fi

# try to find the edit logs whose transaction range covers the txid from the fsimage
# first find the first txid which is greater or equal to the fsimage txid
ending_txid=`ls -1 ${edits_dir} | grep -E "^edits_[[:digit:]]+-[[:digit:]]+\$" | \
    awk -v t="$image_txid" -F'-' '{if ($2 >= t) {print $2}}' | head -1`
if [ -z "$ending_txid" ]; then
  echo "Error; found 0 covering edit files."
  exit 1
fi
# then grep the file ending with the ending_txid, exit if duplicated edits exist
edits_file_count=`ls -1 ${edits_dir} | grep -E "^edits_[[:digit:]]+-0*$ending_txid\$" | wc -l`
if [ "$edits_file_count" != 1 ]; then
  echo "Error; found $edits_file_count matching edit files."
  exit 1
fi
edits_file=`ls -1 ${edits_dir} | grep -E "^edits_[[:digit:]]+-0*$ending_txid\$"`

awk_script='/TIMESTAMP/ { line=$0 } \
      END { match(line, />([[:digit:]]+)</, output); print output[1] }'
echo "Start timestamp for $image_txid is: (this may take a moment)"
hdfs oev -i "$edits_dir/$edits_file" -o >(gawk "$awk_script")
