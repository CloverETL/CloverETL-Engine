#!/bin/bash

if [ "$#" -lt 1 ] ; then
	echo invalid paramenters count
	exit 1
fi 

CONFIG=$1
if [ "$#" -gt 1 ] ; then
	SCENARIOS=$2
else
	SCENARIOS=night.ts
fi

ANT_TARGET=run-scenarios-with-engine-build

echo ${CONFIG} | grep '\-profile$' > /dev/null \
	&& OTHER_OPTIONS="${OTHER_OPTIONS} -Dprofiler.settings=CPURecording;MonitorRecording;ThreadProfiling;VMTelemetryRecording" \
	&& ANT_TARGET=run-scenarios-with-profiler

echo ${SCENARIOS} | grep '\-koule' > /dev/null \
    && OTHER_OPTIONS="${OTHER_OPTIONS} -Drunscenarios.Xmx=-Xmx2048m" \
 
export ANT_OPTS="-Xmx500m"
HUDSON_URL=http://klara.javlin.eu:8081/hudson

CLOVER_VERSION_X_X=`echo $JOB_NAME | sed 's/^cloveretl.engine-tests-\(night\|night-functional\|koule\)-\([0-9]\+\.[0-9]\+\).*$/\2/'`
CLOVER_VERSION_X_X_DASH=`echo $CLOVER_VERSION_X_X | sed 's/\./-/g'`
ENGINE_JOB_NAME=cloveretl.engine-${CLOVER_VERSION_X_X}

# get latestSuccessfulBuildNumber by CLI - replaced by hudson xml api
#LATEST_SCRIPT=/tmp/latestSuccessfulBuildNumber.groovy
#scp cloveretl.engine/hudson/latestSuccessfulBuildNumber.groovy klara:${LATEST_SCRIPT}
#ENGINE_BUILD_NUMBER=`ssh klara "java -jar /data/hudson/war/WEB-INF/hudson-cli.jar -s ${HUDSON_URL} groovy ${LATEST_SCRIPT} ${ENGINE_JOB_NAME}"`
ENGINE_BUILD_NUMBER=`wget -q -O - ${HUDSON_URL}/job/cloveretl.engine-${CLOVER_VERSION_X_X}/lastSuccessfulBuild/api/xml?xpath=/*/number/text%28%29`

wget http://klara.javlin.eu:8081/hudson/job/${ENGINE_JOB_NAME}/${ENGINE_BUILD_NUMBER}/artifact/cloveretl.engine/version.properties
CLOVER_VERSION_X_X_X=`cat version.properties | sed 's/\./-/g'|sed 's/version=//'`

[ -f /home/db2inst/sqllib/db2profile ] \
&& source /home/db2inst/sqllib/db2profile

set

# remove files from previous build
rm -rf version.properties* cloverETL* 
# there may be many files in tmp -> use find instead rm
[ -d /data/bigfiles/tmp ] \
&& find /data/bigfiles/tmp -mindepth 1 -delete


wget http://klara.javlin.eu:8081/hudson/job/${ENGINE_JOB_NAME}/${ENGINE_BUILD_NUMBER}/artifact/cloveretl.engine/dist/cloverETL.rel-${CLOVER_VERSION_X_X_X}.zip
unzip -u -o cloverETL.rel-${CLOVER_VERSION_X_X_X}.zip
rm cloverETL.rel-${CLOVER_VERSION_X_X_X}.zip

svn up svn+ssh://klara/svn/cloveretl.bigdata/branches/release-${CLOVER_VERSION_X_X_DASH} /data/bigfiles/cloveretl-engine-${CLOVER_VERSION_X_X}

cd cloveretl.test.environment
/opt/apache-ant/bin/ant ${ANT_TARGET} \
	-Ddir.engine.build=../cloverETL/lib \
	-Ddir.plugins=../cloverETL/plugins \
	-Dscenarios=${SCENARIOS} \
	-Denvironment.config=${CONFIG} \
	-Dlogpath=/data/cte-logs \
	-Dhudson.link=job/${JOB_NAME}/${BUILD_NUMBER} \
	-Dhudson.engine.link=job/${ENGINE_JOB_NAME}/${ENGINE_BUILD_NUMBER} \
	$OTHER_OPTIONS
	
if  [ "$(hostname)" != "klara" ] ; then
	rsync -rv --remove-source-files /data/cte-logs/ klara:/data/cte-logs
fi
