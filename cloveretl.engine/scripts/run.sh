#! /bin/sh

# this script was inspired by practices gained from ant run scripts (http://ant.apache.org/)

# usage 
# run.bat <engine_arguments> <graph_name.grf>

# Extract clover launch arguments.
clover_exec_args=
for arg in "$@" ; do
	clover_exec_args="$clover_exec_args \"$arg\""
done

if [ -z "$CLOVER_HOME" -o ! -d "$CLOVER_HOME" ] ; then
	## resolve links - $0 may be a link to clover's home
	PRG="$0"
	progname=`basename "$0"`

	# need this for relative symlinks
	while [ -h "$PRG" ] ; do
		ls=`ls -ld "$PRG"`
		link=`expr "$ls" : '.*-> \(.*\)$'`
		if expr "$link" : '/.*' > /dev/null; then
			PRG="$link"
		else
			PRG=`dirname "$PRG"`"/$link"
		fi
	done

	CLOVER_HOME=`dirname "$PRG"`/..
	# make it fully qualified
	CLOVER_HOME=`cd "$CLOVER_HOME" && pwd`
fi

echo $CLOVER_HOME


# set CLOVER_LIB location
CLOVER_LIB="${CLOVER_HOME}/lib"


# set java command
if [ -z "$JAVACMD" ] ; then
	if [ -n "$JAVA_HOME"  ] ; then
		if [ -x "$JAVA_HOME/jre/sh/java" ] ; then
			# IBM's JDK on AIX uses strange locations for the executables
			JAVACMD="$JAVA_HOME/jre/sh/java"
		else
			JAVACMD="$JAVA_HOME/bin/java"
		fi
	else
		JAVACMD=`which java 2> /dev/null `
		if [ -z "$JAVACMD" ] ; then
			JAVACMD=java
		fi
	fi
fi

if [ ! -x "$JAVACMD" ] ; then
	echo "Error: JAVA_HOME is not defined correctly."
	echo "  We cannot execute $JAVACMD"
	exit 1
fi


TRANSFORM_PATH="."

echo $CLOVER_LIB/*

for i in $CLOVER_LIB/*.jar $CLOVER_LIB/*.zip; do
	LOCAL_CLASSPATH="$LOCAL_CLASSPATH:$i"
done;

for i in `ls $CLOVER_LIB/*.jar`; do
echo $i
done;


clover_exec_command="exec \"$JAVACMD\" $CLOVER_OPTS -classpath \"$LOCAL_CLASSPATH\" -Dclover.home=\"$CLOVER_HOME\" org.jetel.main.runGraph -plugins $CLOVER_HOME/plugins $clover_exec_args"
echo $clover_exec_command
eval $clover_exec_command
