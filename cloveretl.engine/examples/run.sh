#!/bin/bash

# ************************************
# usage - ./run.sh <graph_name.grf>
# ***** set this before using script *****
ENGINE_PATH=".."
TRANSFORM_PATH="javaExamples"
# ****************************************

ENGINE_JAR="$ENGINE_PATH/cloveretl.engine.jar"

for i in $ENGINE_PATH/lib/*.jar $ENGINE_PATH/lib/*.zip; do
JARS="$JARS:$i"
done;

java -classpath "$TRANSFORM_PATH:$ENGINE_JAR:$JARS:$JAVA_HOME/lib/tools.jar" org.jetel.main.runGraph -plugins $ENGINE_PATH/plugins $1
