<?xml version="1.0" encoding="windows-1250"?>
<!DOCTYPE TestScenario SYSTEM "testscenario.dtd">
<TestScenario ident="after-commit-windows" description="After commit for Windows ext examples" useJMX="true">
	<GlobalRegEx ident="exception" expression="java.lang.Exception" caseSensitive="false" occurences="0" />
	<GlobalRegEx ident="error" expression="^ERROR" caseSensitive="false" occurences="0" />

	<FunctionalTest ident="MsSqlDataWriter" graphFile="graph/graphMsSqlDataWriter.grf">
		<Property name="CONN_DIR" value="../../cloveretl.test.scenarios/conn" />
		<Property name="BCP_UTILITY_PATH" value="C:/Program Files/Microsoft SQL Server/110/Tools/Binn/bcp.exe"/>
		<Property name="PARAMETERS" value="characterType|errFile=${DATAOUT_DIR}\err.bcp"/>
		<Property name="USER" value="sa"/>
		<Property name="PASS" value="semafor"/>
		<FlatFile outputFile="data-out/err.bcp" supposedFile="supposed-out/err.MsSqlWriter.bcp"/>	                                                                    
		<FlatFile outputFile="data-out/exchange.bcp" supposedFile="supposed-out/exchange.MsSqlWriter.bcp"/>	                                                                    
		<FlatFile outputFile="data-out/out.txt" supposedFile="supposed-out/out.MsSqlWriter.txt"/>	                                                                    
	</FunctionalTest>

</TestScenario>
