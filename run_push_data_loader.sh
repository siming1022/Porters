#!/bin/bash


CLASSPATH=".:conf"
for file in `ls ./lib`
do 
    CLASSPATH+=":./lib/$file"
done 

java -cp $CLASSPATH:Porters.jar com.teamsun.porters.exe.MoveMain "$@" &
