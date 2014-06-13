#!/usr/bin/env groovy

def env = System.getenv()
def ant = new AntBuilder()
init()


def jobName = env['JOB_NAME']
assert jobName
def buildNumber = env['BUILD_NUMBER']
assert buildNumber
def testName
jobNameM = jobName =~ /^(cloveretl\.engine)-((tests-after-commit-windows-java-1.7-Sun|tests-after-commit-proxy-java-1.7-Sun|tests-after-commit-java-8-Sun|tests-after-commit-java-1.6-IBM|tests-night-java-1.6-IBM|tests-night-java-1.6-JRockit|tests-night-functional-java-1.7-Sun|tests-after-commit|tests-reset|tests-performance-java-1.7-Sun|detail)-)?(.+)$/
assert jobNameM.matches() 
jobBasename = jobNameM[0][1]
jobGoal = jobNameM[0][3]
versionSuffix = jobNameM[0][4]

if( !jobGoal ) jobGoal = "after-commit"
runTests = jobGoal.startsWith("tests") && jobGoal.contains("java") 
if( runTests ) {
	testNameM = jobGoal =~ /^tests-(.+)-(java-[^-]+-[^-]+)(-(.*))?$/
	assert testNameM.matches() 
	testName = testNameM[0][1]
	testJVM = testNameM[0][2]
	testOption = testNameM[0][4]
	testConfiguration = "engine-${versionSuffix}_${testJVM}"
	if( testOption ) {
		testConfiguration += "-" + testOption
	}   
	scenarios = testName + ".ts"
}
 
def startTime = new Date();
println "======================= " + startTime
println "====================================================="
println "======= Running CloverETL Engine tests =============="
println "jobBasename   = " + jobBasename 
println "jobGoal   = " + jobGoal
if( runTests ){ 
	println "testName   = " + testName 
	println "testJVM   = " + testJVM
	println "testOption   = " + testOption
} 
println "versionSuffix = " + versionSuffix 
println "buildNumber   = " + buildNumber 
println "====================================================="

//println "Environment variables:"
//System.getenv().each{ println "\t${it}" }

baseD = new File( new File('').absolutePath )
engineD = new File( baseD, "cloveretl.engine" ) 
testEnvironmentD = new File( baseD, "cloveretl.test.environment" ) 

jobIdent = testName ? testName : jobGoal
jobIdent += "-${versionSuffix}"
jobIdent = jobIdent.replaceAll('-', '_').toLowerCase()
new File(baseD, "cloveretl.test.scenarios/jobIdent.prm").write("JOB_IDENT=" + jobIdent)
new File(baseD, "cloveretl.examples/ExtExamples/jobIdent.prm").write("JOB_IDENT=" + jobIdent)

antCustomEnv = ["ANT_OPTS":"-Xmx2048m -XX:MaxPermSize=256m"]
if( !runTests ){
	// compile engine and run some tests
	antBaseD = engineD
	antArgs = [
		"-Dadditional.plugin.list=cloveretl.license.engine,cloveretl.component.hadoop,cloveretl.component.commercial,cloveretl.lookup.commercial,cloveretl.compiler.commercial,cloveretl.quickbase.commercial,cloveretl.tlfunction.commercial,cloveretl.ctlfunction.commercial,cloveretl.addressdoctor.commercial,cloveretl.profiler.commercial,cloveretl.mongodb.commercial,cloveretl.validator.commercial,cloveretl.initiate.engine,cloveretl.spreadsheet.commercial,cloveretl.oem.example.component",
		"-Dcte.logpath=/data/cte-logs",
		"-Dcte.hudson.link=job/${jobName}/${buildNumber}",
		"-Ddir.examples=../cloveretl.examples",
	]
	if( jobGoal == "after-commit" ) {
		antTarget = "reports-hudson"
		antArgs += "-Dcte.environment.config=engine-${versionSuffix}_java-1.7-Sun"
		antArgs += "-Dtest.exclude=org/jetel/graph/ResetTest.java,org/jetel/component/fileoperation/SFTPOperationHandlerTest.java,org/jetel/component/fileoperation/FTPOperationHandlerTest.java,com/opensys/cloveretl/component/EmailFilterTest.java"
		antArgs += "-Druntests-target=runtests-scenario-after-commit"
	} else if( jobGoal == "optimalized"){
		antTarget = "reports-hudson-optimalized"
		antArgs += "-Dcte.environment.config=engine-${versionSuffix}_java-1.6-Sun_optimalized"
		antArgs += "-Dobfuscate.plugin.pattern=cloveretl\\.(?!ctlfunction).*"
		antArgs += "-Druntests-dontrun=true"
		antArgs += "-Druntests-target=runtests-scenario-after-commit-with-engine-classes"
	} else if( jobGoal == "detail"){
		antTarget = "reports-hudson-detail"
		antArgs += "-Dcte.environment.config=engine-${versionSuffix}_java-1.7-Sun_detail"
		antArgs += "-Dtest.exclude=org/jetel/graph/ResetTest.java"
		antArgs += "-Druntests-target=runtests-scenario-after-commit"
	} else if( jobGoal == "tests-reset"){
		antTarget = "runtests-tests-reset"
		antArgs += "-Druntests-plugins-dontrun=true"	
		antArgs += "-Dtest.include=org/jetel/graph/ResetTest.java"
		antCustomEnv["ANT_OPTS"] = antCustomEnv["ANT_OPTS"] + " -XX:MaxPermSize=128m"
		// antArgs += "-Druntests-target=runtests-tests-reset"
	} else {
		println "ERROR: Unknown goal '${jobGoal}'"
		exit 1
	}
	
} else {
	// download engine and run tests only
	antBaseD = testEnvironmentD
	
	engineJobName = "cloveretl.engine-" + versionSuffix
	engineBuildNumber = new URL( env['HUDSON_URL'] + "/job/${engineJobName}/lastSuccessfulBuild/buildNumber").text
	println "engineBuildNumber   = " + engineBuildNumber

        cloverVersionPropertiesURL = env['HUDSON_URL'] + "/job/${engineJobName}/${engineBuildNumber}/artifact/cloveretl.engine/version.properties"
        println "cloverVersionPropertiesURL ${cloverVersionPropertiesURL}"

	cloverVersionProperties = new URL( cloverVersionPropertiesURL ).text
        cloverVersionPropertiesM = cloverVersionProperties =~ /version.product=([^\n]*)/

	cloverVersion = cloverVersionPropertiesM[0][1]
	cloverVersionDash = cloverVersion.replaceAll("\\.","-")
	println "cloverVersion   = " + cloverVersion
	
	antArgs = [
		"-Ddir.engine.build=../cloverETL/lib",
		"-Ddir.plugins=../cloverETL/plugins",
		"-Dscenarios=${scenarios}",
		"-Denvironment.config=${testConfiguration}",
		"-Dlogpath=/data/cte-logs",
		"-Dhudson.link=job/${jobName}/${buildNumber}",
		"-Dhudson.engine.link=job/${engineJobName}/${engineBuildNumber}",
		"-Ddir.examples=../cloveretl.examples",
		"-Dtestenv.etlenvironment=engine",
	]

	antTarget = "run-scenarios-with-engine-build-with-testdb"
	if( testOption == "profile" ){
		antTarget = "run-scenarios-with-profiler"
		antArgs += "-Dprofiler.settings=CPURecording;MonitorRecording;ThreadProfiling;VMTelemetryRecording"
	}
	if( testName == "after-commit-koule" ){
		antArgs += "-Drunscenarios.Xmx=-Xmx2048m"
		antCustomEnv["PATH"]="${env['PATH']}:/home/db2inst/sqllib/bin:/home/db2inst/sqllib/adm:/home/db2inst/sqllib/misc"
		antCustomEnv["DB2DIR"]="/opt/ibm/db2/V9.7"
		antCustomEnv["DB2INSTANCE"]="db2inst"
	}
	if( testName == "after-commit-windows" ){
		//testing derby database is not available under windows platform, so target is changed
		antTarget = "run-scenarios-with-engine-build"
		antArgs += "-Drunscenarios.Xmx=-Xmx512m"
	}
	if( testName == "after-commit-proxy" ){
		antTarget = "run-scenarios-with-engine-build"
		antArgs += "-Drunscenarios.Xmx=-Xmx512m"
		trustStoreF = new File(baseD, "cloveretl.test.scenarios/truststore/proxyTests.truststore")
		antArgs += "-Drunscenarios.trustStore=-Djavax.net.ssl.trustStore=${trustStoreF}"
	}
	if (testName == "night") { //night tests need more memory
		antArgs += "-Drunscenarios.Xmx=-Xmx2048m"
	}
	if (testName == "performance") {
		antArgs += "-Drunscenarios.Xmx=-Xmx2048m"
	}
	if (testName == "after-commit" && testJVM.endsWith("IBM")) {
		// disable Hadoop tests
		antArgs += "-Dexcludedtestidentpattern=Hadoop.*|HDFS.*|Hive.*"
		// prevent OutOfMemoryError and Segmentation error on IBM Java
		//antArgs += "-Drunscenarios.Xmx=-Xmx1024m"
	}

	
	
	cloverD = new File(baseD, "cloverETL")
	// removing files from previous build 
	ant.delete( dir:cloverD )
	ant.delete(failonerror:false){ fileset( dir:"/data/bigfiles/tmp" , includes:"**")}

	engineURL = new URL( env['HUDSON_URL'] + "/job/${engineJobName}/${engineBuildNumber}/artifact/cloveretl.engine/dist/cloverETL.rel-${cloverVersionDash}.zip" )
	engineFile = new File( baseD, "cloverETL.zip" )
	engineURL.download( engineFile )
	ant.unzip( src:engineFile, dest:baseD )
	ant.delete( file:engineFile)

	//"svn up svn+ssh://klara/svn/cloveretl.bigdata/branches/release-${CLOVER_VERSION_X_X_DASH} /data/bigfiles/cloveretl-engine-${CLOVER_VERSION_X_X}".execute()
			
}

assert antTarget
if( env['ComSpec'] ) {
	// windows
	antC = [env['ComSpec'],
		"/c",
		"${env['BASE']}/tools/ant-1.7/bin/ant",
		antTarget
	]
} else {
	// unix
	antC = ["${env['HUDSON_HOME']}/tools/ant-1.7/bin/ant",
		antTarget
	]
}

antArgs.each{arg-> antC += arg}
antC.executeSave(subEnv(antCustomEnv), antBaseD)
	
println "hostName=" + InetAddress.localHost.hostName
if( InetAddress.localHost.hostName != "linda" ) {
	rsyncC = ["rsync", "-rv", "--remove-source-files", "/data/cte-logs/", "jenkins@linda:/data/cte-logs"]
	keyFile = new File("/hudson/id_dsa")
	if( keyFile.exists() ){
		rsyncC += "--rsh=ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i ${keyFile.absolutePath}"
		println "using key ${keyFile.absolutePath}" 
	} else {
		println "using default key" 
	}
	rsyncC.executeRsync()
}



/* some common Groovy extensions */

void init(){
	String.metaClass.executeSave = {
		println "starting command: ${delegate}"
		def p = delegate.execute()
		p.waitForProcessOutput( System.out, System.err )
		assert p.exitValue() == 0
	}
	
	ArrayList.metaClass.executeSave = {
		delegate.executeSave(null, null)
	}
	
	ArrayList.metaClass.executeSave = { procEnv, dir ->
		print "starting ant command: "; delegate.each{ print "'"+it+"' "}; println ""
		def p = delegate.execute(procEnv, dir)
		p.waitForProcessOutput( System.out, System.err )
		assert p.exitValue() == 0
	}
	
	ArrayList.metaClass.executeRsync = {
		print "starting command: "; delegate.each{ print "'"+it+"' "}; println ""
		def p = delegate.execute()
		p.waitForProcessOutput( System.out, System.err )
		assert (p.exitValue() == 0 || p.exitValue() == 24) // rsync exits with 24 if some files vanish - usually ok
	}
	
	URL.metaClass.download = { File toFile ->
		println "downloading ${delegate} to ${toFile}"
	    def out = new BufferedOutputStream(new FileOutputStream(toFile))
	    try {
		    out << delegate.openStream()
		} finally {
    		out.close()
		}
	}
}

def String[] subEnv(m) { 
	n = [:]
	System.getenv().collect {k,v->n[k]=v} 
	m.collect {k,v->n[k]=v} 
	n.collect { k, v -> "$k=$v" }
}


