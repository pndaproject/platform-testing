#!/bin/bash
#
# Please check pnda-build/ for the build products

VERSION=${1}

function error {
    echo "Not Found"
    echo "Please run the build dependency installer script"
    exit -1
}

echo -n "Apache Maven 3.0.5: "
if [[ $(mvn -version 2>&1) == *"Apache Maven 3.0.5"* ]]; then
    echo "OK"
else
    error
fi

mkdir -p pnda-build
mvn versions:set -DnewVersion=${VERSION}
mvn clean package
mv target/platform-testing-cdh-${VERSION}.tar.gz pnda-build/
mv target/platform-testing-general-${VERSION}.tar.gz pnda-build/
sha512sum pnda-build/platform-testing-cdh-${VERSION}.tar.gz > pnda-build/platform-testing-cdh-${VERSION}.tar.gz.sha512.txt
sha512sum pnda-build/platform-testing-general-${VERSION}.tar.gz > pnda-build/platform-testing-general-${VERSION}.tar.gz.sha512.txt
