#!/bin/sh

export ANT_OPTS="-Xmx500m"

set

cd cloveretl.engine

/opt/apache-ant/bin/ant clean build reports-hudson dist \
	-Dadditional.plugin.list=cloveretl.component.commercial,cloveretl.lookup.commercial,cloveretl.compiler.commercial,cloveretl.quickbase.commercial \
	-Dcte.environment.config=trunk_java-1.5-Sun \
	-Dcte.logpath=/data/cte-logs \
	-Dcte.hudson.link=job/$JOB_NAME/$BUILD_NUMBER
	
if  [ "$(hostname)" != "klara" ] ; then
	rsync -rv --remove-source-files /data/cte-logs/ klara:/data/cte-logs
fi
