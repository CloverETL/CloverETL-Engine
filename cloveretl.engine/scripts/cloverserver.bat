@echo off

REM this script was inspired by practices gained from ant run scripts (http://ant.apache.org/)

REM usage 
REM cloverserver.bat <engine_arguments> <graph_name.grf> [ - <java_arguments> ]
REM example:
REM cloverserver.bat -verbose myGraph.grf - -classpath c:\myTransformation

REM split command-line arguments to two sets - clover and jvm arguments
REM and define DERIVED_CLOVER_HOME variable
call "%~dp0"\commonlib.bat %*

echo "%DERIVED_CLOVER_HOME%\bin\clover.bat" -noJMX %CLOVER_CMD_LINE_ARGS% - -server -Xmx1024M -XX:+UseParallelGC -classpath %USER_CLASSPATH% %JAVA_CMD_LINE_ARGS%
"%DERIVED_CLOVER_HOME%\bin\clover.bat" -noJMX %CLOVER_CMD_LINE_ARGS% - -server -Xmx1024M -XX:+UseParallelGC -classpath %USER_CLASSPATH% %JAVA_CMD_LINE_ARGS%
