<?xml version="1.0" encoding="windows-1250"?>
<!DOCTYPE TestScenario SYSTEM "testscenario.dtd">
<TestScenario ident="after-commit-windows" description="After commit for Windows ext examples" useJMX="true">
	<GlobalRegEx ident="exception" expression="java.lang.Exception" caseSensitive="false" occurences="0" />
	<GlobalRegEx ident="error" expression="^ERROR" caseSensitive="false" occurences="0" />

   <FunctionalTest ident="MsSqlDataWriter" graphFile="graph/graphMsSqlDataWriter.grf">
	 <FlatFile outputFile="data-out/err.bcp" supposedFile="supposed-out/err.MsSqlWriter.bcp"/>	                                                                    
	 <FlatFile outputFile="data-out/exchange.bcp" supposedFile="supposed-out/exchange.MsSqlWriter.bcp"/>	                                                                    
	 <FlatFile outputFile="data-out/out.txt" supposedFile="supposed-out/out.MsSqlWriter.txt"/>	                                                                    
    </FunctionalTest>
   

</TestScenario>
