#!/bin/sh

set
cd cloveretl.engine
/opt/apache-ant-1.7.0/bin/ant clean build reports-hudson dist \
	-Dadditional.plugin.list=cloveretl.lookup.commercial \
	-Dcte.environment.config=engine-2.6_java-1.5-Sun \
	-Dlogpath=/data/cte-logs \
	-Dcte.hudson.link=$JOB_NAME/$BUILD_NUMBER