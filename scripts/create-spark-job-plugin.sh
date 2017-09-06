#!/bin/bash
#
# Simple copy & replace script to create a new spark job plugin src files.
#
if [ $# -ne 4 ]; then
    echo "usage: $0 src-dir src-version dst-dir dst-version"
    echo "example: $0 com.knime.bigdata.spark2_0/src 2_0 com.knime.bigdata.spark2_1/src 2_1"
    exit 1
fi

SRC=$1
SRC_VERSION=$2
DST=$3
DST_VERSION=$4

if [ -e "$3" ]; then
    echo "Destination exists, remove destination befor using $0".
    exit 1
fi

# copy src folder
cp -ar "$SRC" "$DST"

# rename directories
for dir in $(find "$DST" -type d -name "*${SRC_VERSION}*"); do
    mv "$dir" $(echo "$dir" | sed -e "s/$SRC_VERSION/$DST_VERSION/")
done

# rename files
for file in $(find "$DST" -type f -name "*${SRC_VERSION}*"); do
    mv "$file" $(echo "$file" | sed -e "s/$SRC_VERSION/$DST_VERSION/")
done

# change version in files
find "$DST" -type f -print0 | xargs -0 -n 1 sed -i "s/$SRC_VERSION/$DST_VERSION/g"
