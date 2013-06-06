<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE TestScenario SYSTEM "testscenario.dtd">

<TestScenario ident="BigDataExamples" description="BigDataExamples distributed with Designer" useJMX="true">
	<GlobalRegEx ident="error" expression="^ERROR" caseSensitive="false" occurences="0" />
	
	<FunctionalTest ident="UniqueVisits-CloverETL" graphFile="jobflow/UniqueVisits-CloverETL.jbf" excludedEtlEnvironment="engine">
		<FlatFile outputFile="data-tmp/unique_visitors.txt" supposedFile="supposed-out/unique_visitors.txt"/>
		<DeleteFile file="data-tmp/unique_visitors.txt"/>
	</FunctionalTest>
	
	<FunctionalTest ident="UniqueVisits-HadoopHive" graphFile="jobflow/UniqueVisits-HadoopHive.jbf" excludedEtlEnvironment="engine">
		<Property name="CONN_DIR" value="../../cloveretl.test.scenarios/conn" />
		<Property name="LIB_DIR" value="../../cloveretl.test.scenarios/lib" />
		<Property name="HADOOP_JOBTRACKER_HOST" value="virt-hotel.javlin.eu" />
		<Property name="HADOOP_HIVE_URL" value="jdbc\:hive\://virt-hotel.javlin.eu\:10000/default" />
		<Property name="HADOOP_NAMENODE_HOST" value="virt-hotel.javlin.eu" />
		<FlatFile outputFile="data-tmp/unique_visitors.txt" supposedFile="supposed-out/unique_visitors.txt"/>
		<DeleteFile file="data-tmp/unique_visitors.txt"/>
	</FunctionalTest>
	
	<FunctionalTest ident="UniqueVisits-HadoopMapReduce" graphFile="jobflow/UniqueVisits-HadoopMapReduce.jbf" excludedEtlEnvironment="engine">
		<Property name="CONN_DIR" value="../../cloveretl.test.scenarios/conn" />
		<Property name="LIB_DIR" value="../../cloveretl.test.scenarios/lib" />
		<Property name="HADOOP_JOBTRACKER_HOST" value="virt-hotel.javlin.eu" />
		<Property name="HADOOP_HIVE_URL" value="jdbc\:hive\://virt-hotel.javlin.eu\:10000/default" />
		<Property name="HADOOP_NAMENODE_HOST" value="virt-hotel.javlin.eu" />
		<FlatFile outputFile="data-tmp/unique_visitors.txt" supposedFile="supposed-out/unique_visitors.txt"/>
		<DeleteFile file="data-tmp/unique_visitors.txt"/>
	</FunctionalTest>

</TestScenario>
