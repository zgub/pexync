#!/bin/bash

for i in `ls testfiles` 
do
    echo -n `md5sum testfiles/$i`
    FILESIZE=$(stat -c%s "testfiles/$i")
    echo " $FILESIZE"
    echo -n `md5sum Xync/$i`
    FILESIZE=$(stat -c%s "Xync/$i")
    echo " $FILESIZE"
    echo
done