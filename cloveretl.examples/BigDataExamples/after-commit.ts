<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE TestScenario SYSTEM "testscenario.dtd">

<TestScenario ident="BigDataExamples" description="BigDataExamples distributed with Designer" useJMX="true">
	<GlobalRegEx ident="error" expression="^ERROR" caseSensitive="false" occurences="0" />
	
	<FunctionalTest ident="UniqueVisits-CloverETL" graphFile="jobflow/UniqueVisits-CloverETL.jbf" excludedEtlEnvironment="engine">
		<FlatFile outputFile="data-tmp/unique_visitors.txt" supposedFile="supposed-out/unique_visitors.txt"/>
		<DeleteFile file="data-tmp/unique_visitors.txt"/>
	</FunctionalTest>
	
	<!-- this is not a test, it only sets correct CONN_DIR to workspace.prm (the graph is not a part of the examples
		and thus it is placed in data-tmp -->
	<FunctionalTest ident="SetConnDir" graphFile="data-tmp/setConnDir.grf" excludedEtlEnvironment="engine" />
	
	<FunctionalTest ident="UniqueVisits-MongoDB" graphFile="jobflow/UniqueVisits-MongoDB.jbf" excludedEtlEnvironment="engine">
		<FlatFile outputFile="data-tmp/unique_visitors.txt" supposedFile="supposed-out/unique_visitors_mongo.txt"/>
		<DeleteFile file="data-tmp/unique_visitors.txt"/>
	</FunctionalTest>

	<!-- this is not a test, it only sets correct parameters to hadoop.prm (the graph is not a part of the examples
		and thus it is placed in data-tmp -->
	<FunctionalTest ident="SetHadoopParameters" graphFile="data-tmp/setParameters.grf" excludedEtlEnvironment="engine" />
	
	<FunctionalTest ident="UniqueVisits-HadoopHive" graphFile="jobflow/UniqueVisits-HadoopHive.jbf" excludedEtlEnvironment="engine" excludedContainers="websphere85">
		<FlatFile outputFile="data-tmp/unique_visitors.txt" supposedFile="supposed-out/unique_visitors.txt"/>
		<DeleteFile file="data-tmp/unique_visitors.txt"/>
	</FunctionalTest>
	
	<FunctionalTest ident="UniqueVisits-HadoopMapReduce" graphFile="jobflow/UniqueVisits-HadoopMapReduce.jbf" excludedEtlEnvironment="engine" excludedContainers="websphere85">
		<FlatFile outputFile="data-tmp/unique_visitors.txt" supposedFile="supposed-out/unique_visitors.txt"/>
		<DeleteFile file="data-tmp/unique_visitors.txt"/>
	</FunctionalTest>

</TestScenario>
