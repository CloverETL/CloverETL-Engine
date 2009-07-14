#!/bin/sh

set
cd cloveretl.engine
/opt/apache-ant-1.7.0/bin/ant clean build reports-hudson-coverage \
	-Dadditional.plugin.list=cloveretl.lookup.commercial \
	-Dcte.usedb=false \
	-Dlogpath=/data/cte-logs \
	-Dcte.hudson.link=$JOB_NAME/$BUILD_NUMBER