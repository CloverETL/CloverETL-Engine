#!/bin/sh

set -e
set -u

export ANT_OPTS="-Xmx500m"

set

cd cloveretl.engine

/opt/apache-ant/bin/ant clean runtests-with-testdb \
	-Dadditional.plugin.list=cloveretl.component.commercial,cloveretl.lookup.commercial,cloveretl.compiler.commercial,cloveretl.quickbase.commercial,cloveretl.tlfunction.commercial,cloveretl.ctlfunction.commercial\
	-Dtest.include=org/jetel/graph/ResetTest.java \
	-Ddir.examples=../cloveretl.examples \
	-Druntests-plugins-dontrun=true
	