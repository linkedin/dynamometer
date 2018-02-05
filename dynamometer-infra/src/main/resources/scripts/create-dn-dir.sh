#!/usr/bin/env bash
#
# Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.
#

# Script for creating Dynamometer DataNode directories within YARN container.
# Usage: ./create-dn-dir.sh data_dir_list
#        data_dir_list is a comma-separated list of data directories to populate

dataDirs="$1"
# Strip off file:// prefix if present; convert to array
dataDirs=${dataDirs//file:\/\//}
dataDirs=(${dataDirs//,/ })

layoutVersion=$(${HADOOP_HOME}/bin/hadoop jar dynamometer.jar \
                com.linkedin.dynamometer.DataNodeLayoutVersionFetcher)
if [ "$layoutVersion" = -56 ]; then
  layoutDirectoryMax=255
elif [ "$layoutVersion" = -57 ]; then
  layoutDirectoryMax=31
else
  echo "Invalid/unsupported layout version: $layoutVersion"
  exit 1
fi

# We call this periodically so that if the application is killed before this completes,
# the script will exit itself rather than continuing to create new blocks
parentPID=$PPID
function exitIfParentIsDead() {
  if ! kill -0 ${parentPID} 2>/dev/null; then
    echo "Parent process died; exiting from ${0##*/}"
    exit 1
  fi
}

set -m

metafile=`pwd`/scripts/metafile
versionFile=`pwd`/VERSION

bpId=`cat ${versionFile} | grep blockpoolID | awk -F\= '{print $2}'`
clusterId=`cat ${versionFile} | grep clusterID | awk -F\= '{print $2}'`
namespaceID=`cat ${versionFile} | grep namespaceID | awk -F\= '{print $2}'`

chmod 644 ${metafile}

dnUUID=$(uuidgen)

for dataDir in ${dataDirs[@]}; do
  mkdir -p "$dataDir/current"
  cat > "$dataDir/current/VERSION" << EOF
clusterID=${clusterId}
namespaceID=${namespaceID}
cTime=0
blockpoolID=${bpId}
datanodeUuid=${dnUUID}
storageID=DS-$(uuidgen)
storageType=DATA_NODE
layoutVersion=${layoutVersion}
EOF
  bpDir="$dataDir/current/$bpId/current"
  mkdir -p ${bpDir}
  mkdir ${bpDir}/rbw
  cp "$dataDir/current/VERSION" ${bpDir}/VERSION
  finDir=${bpDir}/finalized
  mkdir ${finDir}

  for p in `seq 0 ${layoutDirectoryMax}`; do
    mkdir_args=()
    for q in `seq 0 ${layoutDirectoryMax}`; do
      mkdir_args[$((q+1))]="${finDir}/subdir${p}/subdir${q}"
    done
    mkdir -p "${mkdir_args[@]}"
    exitIfParentIsDead
  done
done

exitIfParentIsDead

while [ 1 ]; do fg 2> /dev/null; [ $? == 1 ] && break; done

exitIfParentIsDead

blkList=`pwd`/block
blkIdx=0

blkListSplitDir="`pwd`/blkListSplit"
mkdir -p "$blkListSplitDir"

max_parallelism=100
lines_per_split=5000
blkCnt=`wc -l "$blkList" | awk '{ print $1 }'`
if [ ${blkCnt} -gt $((max_parallelism * lines_per_split)) ]; then
  lines_per_split=$((blkCnt / max_parallelism))
fi

echo "Beginning to create $blkCnt blocks using $lines_per_split blocks per process"

split -a 3 -l "$lines_per_split" "$blkList" "$blkListSplitDir/blkList"

function read_split() {
  splitFile="$1"
  blkIdx=0

  while read blkInfo; do
      dataDirIdx=$((blkIdx % ${#dataDirs[@]}))
      dataDir=${dataDirs[$dataDirIdx]}/current/${bpId}/current/finalized

      blkInfoArray=(${blkInfo//,/ })
      blkId=${blkInfoArray[0]}
      blkGs=${blkInfoArray[1]}
      blkSz=${blkInfoArray[2]}

      d1=$(( (blkId>>16) & layoutDirectoryMax ))
      d2=$(( (blkId>>8) & layoutDirectoryMax ))

      blkDir=${dataDir}/subdir${d1}/subdir${d2}

      truncate -s ${blkSz} ${blkDir}/blk_${blkId}
      ln -s ${metafile} ${blkDir}/blk_${blkId}_${blkGs}.meta

      blkIdx=$((blkIdx+1))

      if [ $((blkIdx % 1000)) == 0 ]; then
        echo "For split file '$splitFile', created $blkIdx of $lines_per_split blocks"
        exitIfParentIsDead
      fi

  done < "$splitFile"
}

for splitFile in "$blkListSplitDir/blkList"*; do
  read_split "$splitFile" &
done

while [ 1 ]; do fg 2> /dev/null; [ $? == 1 ] && break; done

echo "Finished creating $blkCnt blocks"

rm -rf "$blkListSplitDir"