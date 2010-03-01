#!/bin/sh

set -e
set -u

export ANT_OPTS="-Xmx500m"

CLOVER_VERSION_X_X=`echo $JOB_NAME | sed 's/^cloveretl.engine-\([0-9]\+\.[0-9]\+\).*$/\1/'`

set

cd cloveretl.engine

/opt/apache-ant/bin/ant clean build reports-hudson dist \
	-Dadditional.plugin.list=cloveretl.component.commercial,cloveretl.lookup.commercial,cloveretl.compiler.commercial,cloveretl.quickbase.commercial \
	-Dcte.environment.config=${CLOVER_VERSION_X_X}_java-1.5-Sun \
	-Dcte.logpath=/data/cte-logs \
	-Dcte.hudson.link=job/$JOB_NAME/$BUILD_NUMBER
	
if  [ "$(hostname)" != "klara" ] ; then
	rsync -rv --remove-source-files /data/cte-logs/ klara:/data/cte-logs
fi
