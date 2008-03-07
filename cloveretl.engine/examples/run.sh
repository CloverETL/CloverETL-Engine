#!/bin/bash

# ************************************
# usage - ./run.sh <graph_name.grf>
# ***** set this before using script *****
ENGINE_PATH=".."
TRANSFORM_PATH="trans"
# ****************************************

ENGINE_JAR="$ENGINE_PATH/lib/cloveretl.engine.jar"

for i in $ENGINE_PATH/lib/*.jar $ENGINE_PATH/lib/*.zip; do
JARS="$JARS:$i"
done;

java -classpath "$TRANSFORM_PATH:$ENGINE_JAR:$JARS" org.jetel.main.runGraph -plugins $ENGINE_PATH/plugins graph/$1
