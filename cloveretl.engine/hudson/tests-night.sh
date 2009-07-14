#!/bin/sh

CONFIG=$1
 
export ANT_OPTS="-Xmx500m"
set
wget http://klara.javlin.eu:8081/hudson/job/cloveretl.engine-2.6/lastSuccessfulBuild/artifact/cloveretl.engine/dist/cloverETL.rel-2-6-4.zip
unzip -u -o cloverETL.rel-2-6-4.zip
rm cloverETL.rel-2-6-4.zip
cd cloveretl.test.environment
/opt/apache-ant-1.7.0/bin/ant run-scenarios-with-engine-build \
	-Ddir.engine.build=../cloverETL/lib \
	-Ddir.plugins=../cloverETL/plugins \
	-Dscenarios=night.ts \
	-Dcte.environment.config=${CONFIG} \
	-Ddir.scenarios=../cloveretl.test.scenarios \
	-Dlogpath=/data/cte-logs \
	-Dcte.hudson.link=$JOB_NAME/$BUILD_NUMBER
scp -r /data/cte-logs klara:/data && cd /data/cte-logs && rm -r -f *