#!/bin/sh

# ************************************
# usage - run.bat <graph_name.grf>

# ***** set this before using script *****
JAVA_HOME="/opt/jdk1.5"
ENGINE_PATH="."
LIBS_PATH="./lib"
TRANSFORM_PATH="."
# ****************************************

ENGINE_JAR="$ENGINE_PATH/cloveretl.engine.jar"

for i in $LIBS_PATH/*.jar $LIBS_PATH/*.zip; do
JARS="$JARS:$i"
done;

java -classpath "$TRANSFORM_PATH:$ENGINE_JAR:$JARS:$JAVA_HOME/lib/tools.jar" org.jetel.main.runGraph $1
