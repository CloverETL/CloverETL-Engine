#!/usr/bin/env groovy

def env=System.getenv()
def ant = new AntBuilder() 

def jobName = env['JOB_NAME']
assert jobName
def buildNumber = env['BUILD_NUMBER']
assert buildNumber
jobNameM = jobName =~ /^(cloveretl\.engine)-((.*)-)?([^-]*)$/
assert jobNameM.matches() 
jobBasename = jobNameM[0][1]
jobGoal = jobNameM[0][3]
versionSuffix = jobNameM[0][4]

if( !jobGoal ) jobGoal = "after-commit"
 
def startTime = new Date();
println "======================= " + startTime
println "====================================================="
println "======= Running CloverETL Server tests =============="
println "JOB_BASENAME   = " + jobBasename 
println "JOB_GOAL   = " + jobGoal 
println "VERSION_SUFFIX = " + versionSuffix 
println "BUILD_NUMBER   = " + buildNumber 
println "====================================================="

//println "Environment variables:"
//System.getenv().each{ println "\t${it}" }

baseD = new File( new File('').absolutePath)
engineD = new File(baseD, "cloveretl.engine") 

def antArgs = [
	"-Dadditional.plugin.list=cloveretl.component.commercial,cloveretl.lookup.commercial,cloveretl.compiler.commercial,cloveretl.quickbase.commercial,cloveretl.tlfunction.commercial,cloveretl.ctlfunction.commercial",
	"-Dcte.logpath=/data/cte-logs",
	"-Dcte.hudson.link=job/jobName/${buildNumber}",
	"-Dtest.exclude=org/jetel/graph/ResetTest.java",
	"-Ddir.examples=../cloveretl.examples",
]
if( jobGoal == "after-commit" ) {
	antTarget = "reports-hudson"
	antArgs += "-Dcte.environment.config=engine-${versionSuffix}_java-1.6-Sun"
} else if( jobGoal == "optimalized"){
	antTarget = "reports-hudson-optimalized"
	antArgs += "-Dcte.environment.config=engine-${versionSuffix}_java-1.6-Sun_optimalized"
	antArgs += "-Dobfuscate.plugin.pattern=cloveretl.*"
	antArgs += "-Druntests-dontrun=true"
} else {
	println "ERROR: Unknown goal '${jobGoal}'"
	exit 1
}
assert antTarget

antC = ["${env['HUDSON_HOME']}/tools/ant-1.7/bin/ant",
	antTarget
]
antArgs.each{arg-> antC += arg}

print "starting ant command: "; antC.each{ print "'"+it+"' "}; println ""
antP = antC.execute(subEnv(["ANT_OPTS":"-Xmx500m"]), engineD)
antP.waitForProcessOutput( System.out, System.err )
assert antP.exitValue() == 0
	
if( env['HOST_NAME'] != "klara" ) {
	"rsync -rv --remove-source-files /data/cte-logs/ klara:/data/cte-logs".executeSave()
}

def String[] subEnv(m) { 
	n = [:]
	System.getenv().collect {k,v->n[k]=v} 
	m.collect {k,v->n[k]=v} 
	n.collect { k, v -> "$k=$v" }
}

String.metaClass.executeSave = {
	def p = delegate.execute()
	p.waitForProcessOutput( System.out, System.err )
	assert p.exitValue() == 0
}
