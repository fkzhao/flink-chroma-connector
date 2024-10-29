#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

##############################################################
# This script is used to compile Flink-Chroma-Connector
# Usage:
#    sh build.sh
#
##############################################################

selectFlink() {
  echo 'Flink-Chroma-Connector supports multiple versions of flink. Which version do you need ?'
  select flink in "1.15.x" "1.16.x" "1.17.x" "1.18.x" "1.19.x"
  do
    case $flink in
      "1.15.x")
        return 1
        ;;
      "1.16.x")
        return 2
        ;;
      "1.17.x")
        return 3
        ;;
      "1.18.x")
        return 4
        ;;
      "1.19.x")
        return 5
        ;;
      *)
        echo "invalid selected, exit.."
        exit 1
        ;;
    esac
  done
}

FLINK_VERSION=0
selectFlink
flinkVer=$?

if [ ${flinkVer} -eq 1 ]; then
    FLINK_VERSION="1.15.2"
elif [ ${flinkVer} -eq 2 ]; then
    FLINK_VERSION="1.16.0"
elif [ ${flinkVer} -eq 3 ]; then
    FLINK_VERSION="1.17.0"
elif [ ${flinkVer} -eq 4 ]; then
    FLINK_VERSION="1.18.0"
elif [ ${flinkVer} -eq 5 ]; then
    FLINK_VERSION="1.19.0"
fi

FLINK_MAJOR_VERSION=0
[ ${FLINK_VERSION} != 0 ] && FLINK_MAJOR_VERSION=${FLINK_VERSION%.*}

echo " flink version: ${FLINK_VERSION}, major version: ${FLINK_MAJOR_VERSION}"
echo " build starting..."

mvn clean package -Dflink.version=${FLINK_VERSION} -Dflink.major.version=${FLINK_MAJOR_VERSION} "$@"
ROOT=$(pwd)
EXIT_CODE=$?
if [ $EXIT_CODE -eq 0 ]; then
  OUTPUT_DIR=${ROOT}/output
  [ ! -d "$OUTPUT_DIR" ] && mkdir "$OUTPUT_DIR"
  dist_jar=$(ls "${ROOT}"/target | grep "flink-chroma-" | grep -v "sources.jar" | grep -v "original-")
  rm -rf "${OUTPUT_DIR}"/"${dist_jar}"
  cp "${ROOT}"/target/"${dist_jar}" "$OUTPUT_DIR"
  echo "*****************************************************************"
  echo "Successfully build Flink-Chroma-Connector"
  echo "dist: $OUTPUT_DIR/$dist_jar "
  echo "*****************************************************************"
  exit 0
else
  echo_w "Failed build Flink-Chroma-Connector"
  exit $EXIT_CODE
fi