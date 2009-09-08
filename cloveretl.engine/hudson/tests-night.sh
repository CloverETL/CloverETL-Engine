#!/bin/sh

if [ "$#" -lt 1 ] ; then
	echo invalid paramenters count
	exit 1;
fi 

CONFIG=$1
if [ "$#" -gt 1 ] ; then
	SCENARIOS=$2
else
	SCENARIOS=night.ts
fi
 
export ANT_OPTS="-Xmx500m"

set

# remove files from previous build
rm -rf version.properties* cloverETL* 
# there may be many files in tmp -> use find instead rm
find /data/bigfiles/tmp -mindepth 1 -delete

CLOVER_VERSION_J=`echo $JOB_NAME |sed 's/^cloveretl.engine-tests-night-\(functional-\)\?\([^-]*\).*$/\2/'`
wget http://klara.javlin.eu:8081/hudson/job/cloveretl.engine-${CLOVER_VERSION_J}/lastSuccessfulBuild/artifact/cloveretl.engine/version.properties
CLOVER_VERSION_D=`cat version.properties | sed 's/\./-/g'|sed 's/version=//'`

wget http://klara.javlin.eu:8081/hudson/job/cloveretl.engine-${CLOVER_VERSION_J}/lastSuccessfulBuild/artifact/cloveretl.engine/dist/cloverETL.rel-${CLOVER_VERSION_D}.zip
unzip -u -o cloverETL.rel-${CLOVER_VERSION_D}.zip
rm cloverETL.rel-${CLOVER_VERSION_D}.zip

cd cloveretl.test.environment
/opt/apache-ant/bin/ant run-scenarios-with-engine-build \
	-Ddir.engine.build=../cloverETL/lib \
	-Ddir.plugins=../cloverETL/plugins \
	-Dscenarios=${SCENARIOS} \
	-Denvironment.config=${CONFIG} \
	-Dlogpath=/data/cte-logs \
	-Dhudson.link=job/$JOB_NAME/$BUILD_NUMBER
	
if  [ "$(hostname)" != "klara" ] ; then
	rsync -rv --remove-source-files /data/cte-logs/ klara:/data/cte-logs
fi
