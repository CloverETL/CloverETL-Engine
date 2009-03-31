@echo off

REM this script was inspired by practices gained from ant run scripts (http://ant.apache.org/)

REM usage 
REM clover.bat <engine_arguments> <graph_name.grf> [ - <java_arguments> ]
REM example:
REM clover.bat -noJMX myGraph.grf - -server -classpath c:\myTransformation

REM split command-line arguments to two sets - clover and jvm arguments
REM and define CLOVER_HOME variable
call commonlib.bat %*

set _JAVACMD=%JAVACMD%
set TOOLS_JAR=%JAVA_HOME%\lib\tools.jar

if "%JAVA_HOME%" == "" goto noJavaHome
if not exist "%JAVA_HOME%\bin\java.exe" goto noJavaHome
if "%_JAVACMD%" == "" set _JAVACMD=%JAVA_HOME%\bin\java.exe
goto runClover

:noJavaHome
if "%_JAVACMD%" == "" set _JAVACMD=java.exe
set TOOLS_JAR=

:runClover
set TRANSFORM_PATH=%~dp0

set COMMONS_LOGGING_JAR=%CLOVER_HOME%\lib\commons-logging.jar
set JAVOLUTION_JAR=%CLOVER_HOME%\lib\javolution.jar
set JMS_JAR=%CLOVER_HOME%\lib\jms.jar
set JXL_JAR=%CLOVER_HOME%\lib\jxl.jar
set LOG4J_JAR=%CLOVER_HOME%\lib\log4j-1.2.12.jar
set COMMONS_CLI_JAR=%CLOVER_HOME%\lib\commons-cli-1.0.jar
set JSCH_JAR=%CLOVER_HOME%\lib\jsch-0.1.34.jar
set JANINO_JAR=%CLOVER_HOME%\lib\janino.jar

echo "%_JAVACMD%" %CLOVER_OPTS% %JAVA_CMD_LINE_ARGS% -classpath "%CLASSPATH%;%USER_CLASSPATH%;%TRANSFORM_PATH%;%CLOVER_HOME%\lib\cloveretl.engine.jar;%COMMONS_LOGGING_JAR%;%JAVOLUTION_JAR%;%JMS_JAR%;%JXL_JAR%;%LOG4J_JAR%;%TOOLS_JAR%;%COMMONS_CLI_JAR%;%JSCH_JAR%;%JANINO_JAR%" "-Dclover.home=%CLOVER_HOME%" org.jetel.main.runGraph -plugins "%CLOVER_HOME%\plugins" %CLOVER_CMD_LINE_ARGS%
"%_JAVACMD%" %CLOVER_OPTS% %JAVA_CMD_LINE_ARGS% -classpath "%CLASSPATH%;%USER_CLASSPATH%;%TRANSFORM_PATH%;%CLOVER_HOME%\lib\cloveretl.engine.jar;%COMMONS_LOGGING_JAR%;%JAVOLUTION_JAR%;%JMS_JAR%;%JXL_JAR%;%LOG4J_JAR%;%TOOLS_JAR%;%COMMONS_CLI_JAR%;%JSCH_JAR%;%JANINO_JAR%" "-Dclover.home=%CLOVER_HOME%" org.jetel.main.runGraph -plugins "%CLOVER_HOME%\plugins" %CLOVER_CMD_LINE_ARGS%
set RETURN_CODE=%ERRORLEVEL%

set CLOVER_OPTS=
set CLOVER_CMD_LINE_ARGS=
set JAVA_CMD_LINE_ARGS=
set USER_CLASSPATH=
set WAS_CLASSPATH=
set _JAVACMD=
set TOOLS_JAR=
set TRANSFORM_PATH=
set COMMONS_LOGGING_JAR=
set JAVOLUTION_JAR=
set JMS_JAR=
set JXL_JAR=
set LOG4J_JAR=
set COMMONS_CLI_JAR=
set JSCH_JAR=
set JANINO_JAR=

exit /B %RETURN_CODE%
