#!/bin/sh

export ANT_OPTS="-Xmx500m"

set

cd cloveretl.engine

/opt/apache-ant/bin/ant clean reports-hudson-detail \
	-Dadditional.plugin.list=cloveretl.component.commercial,cloveretl.lookup.commercial \
	-Dcte.environment.config=2.8_detail \
	-Dcte.logpath=/data/cte-logs \
	-Dcte.hudson.link=job/$JOB_NAME/$BUILD_NUMBER
	-Dtest.exclude=org/jetel/graph/ResetTest.java
	
if  [ "$(hostname)" != "klara" ] ; then
	rsync -rv --remove-source-files /data/cte-logs/ klara:/data/cte-logs
fi
