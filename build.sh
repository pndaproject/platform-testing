#!/bin/bash
#
# Please check pnda-build/ for the build products

VERSION=${1}
HADOOP_FLAVOR=${2}

function error {
    echo "Not Found"
    echo "Please run the build dependency installer script"
    exit -1
}

function code_quality_error {
    echo "${1}"
}

echo -n "Apache Maven 3.0.5: "
if [[ $(mvn -version 2>&1) == *"Apache Maven 3.0.5"* ]]; then
    echo "OK"
else
    error
fi

BASE=${PWD}

echo -n "Code quality: "
cd src/main/resources
PYLINTOUT=$(find . -type f -name '*.py' | grep -vi __init__ | xargs pylint)
SCORE=$(echo ${PYLINTOUT} | grep -Po '(?<=rated at ).*?(?=/10)')
echo ${SCORE}
if [[ $(bc <<< "${SCORE} > 9") == 0 ]]; then
    code_quality_error "${PYLINTOUT}"
fi

cd ${BASE}/src/main/resources

# Unit tests
PYTHONPATH=${PWD}
find . -type f -name 'unittests.py' | xargs nosetests
[[ $? -ne 0 ]] && exit -1

cd ${BASE}
# Build
mkdir -p pnda-build
mvn versions:set -DnewVersion=${VERSION}
mvn clean package -P${HADOOP_FLAVOR}
mv target/platform-testing-${HADOOP_FLAVOR}-${VERSION}.tar.gz pnda-build/
mv target/platform-testing-general-${VERSION}.tar.gz pnda-build/
sha512sum pnda-build/platform-testing-${HADOOP_FLAVOR}-${VERSION}.tar.gz > pnda-build/platform-testing-${HADOOP_FLAVOR}-${VERSION}.tar.gz.sha512.txt
sha512sum pnda-build/platform-testing-general-${VERSION}.tar.gz > pnda-build/platform-testing-general-${VERSION}.tar.gz.sha512.txt

