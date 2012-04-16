#!/bin/bash

HADOOP_LIB_DIR=tmp/lib
HADOOP_CONF_DIR=.libs/conf

CLASSPATH="${HADOOP_CONF_DIR}"
CLASSPATH=${CLASSPATH}:$JAVA_HOME/lib/tools.jar

for jr in `ls .libs/javalibs/*.jar`;do
CLASSPATH=$jr:$CLASSPATH;done

go test