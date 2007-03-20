#!/bin/sh

# ************************************
# usage - runShowData.bat <graph_name.grf> <component_id>

# ***** set this before using script *****
JAVA_HOME="/usr/lib64/jvm/java-1.5.0-sun-1.5.0_update8"
ENGINE_PATH="."
LIBS_PATH="./lib"
TRANSFORM_PATH="."
# ****************************************

ENGINE_JAR="$ENGINE_PATH/cloveretl.engine.jar"

# for i in $LIBS_PATH/*.jar $LIBS_PATH/*.zip; do
for i in $(ls lib); do
JARS="$JARS:./lib/$i";
#JARS="$JARS:$i";
done;

java -classpath "$TRANSFORM_PATH:$ENGINE_JAR:$JARS:$JAVA_HOME/lib/tools.jar" org.jetel.main.showData $1 $2
