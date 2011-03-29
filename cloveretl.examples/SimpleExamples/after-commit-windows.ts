<?xml version="1.0" encoding="windows-1250"?>
<!DOCTYPE TestScenario SYSTEM "testscenario.dtd">
<TestScenario ident="after-commit-windows" description="After commit for Windows simple examples" useJMX="true">
	<GlobalRegEx ident="exception" expression="java.lang.Exception" caseSensitive="false" occurences="0" />
	<GlobalRegEx ident="error" expression="^ERROR" caseSensitive="false" occurences="0" />

   <FunctionalTest ident="SystemExecute" graphFile="graph/graphSystemExecuteWin.grf">
	 <FlatFile outputFile="data-out/command.out" supposedFile="supposed-out/command.SystemExecuteWin.out"/>	                                                                    
	 <FlatFile outputFile="data-out/dir_command_File.out" supposedFile="supposed-out/dir_command_File.SystemExecuteWin.out"/>	                                                                    
	 <FlatFile outputFile="data-out/dir_command_Port.out" supposedFile="supposed-out/dir_command_Port.SystemExecuteWin.out"/>	                                                                    
    </FunctionalTest>
   

</TestScenario>
