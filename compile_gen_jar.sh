#!/bin/bash

CLASSPATH=".:conf"
for file in `ls ./lib`
do 
    CLASSPATH+=":./lib/$file"
done 

FILELIST=""
for javafile in `find|grep java$`
do
    FILELIST+="$javafile "
done

if [ -e classes ]; then
    rm classes/* -rf
else
    mkdir classes
fi 
javac -verbose -sourcepath src -cp $CLASSPATH -d classes $FILELIST

rm Porters.jar -f
cd classes 
jar cvf ../Porters.jar .
cd - 

