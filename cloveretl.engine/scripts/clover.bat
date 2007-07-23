@echo off

REM this script was inspired by practices gained from ant run scripts (http://ant.apache.org/)

REM usage 
REM clover.bat <engine_arguments> <graph_name.grf>


REM prepare CLOVER_HOME environmental variable
REM %~dp0 is expanded pathname of the current script under NT
set DEFAULT_CLOVER_HOME=%~dp0..

if "%CLOVER_HOME%"=="" set CLOVER_HOME=%DEFAULT_CLOVER_HOME%
set DEFAULT_CLOVER_HOME=


REM Prepare the command line arguments. This loop allows for an unlimited number
REM of arguments (up to the command line limit, anyway).
set CLOVER_CMD_LINE_ARGS=%1
if ""%1""=="""" goto doneStart
shift
:setupArgs
if ""%1""=="""" goto doneStart
set CLOVER_CMD_LINE_ARGS=%CLOVER_CMD_LINE_ARGS% %1
shift
goto setupArgs

:doneStart
REM find CLOVER_HOME if it does not exist due to either an invalid value passed
REM by the user or the %0 problem on Windows 9x
if exist "%CLOVER_HOME%\lib\cloveretl.engine.jar" goto checkJava

REM check for clover in Program Files
if not exist "%ProgramFiles%\cloverETL" goto checkSystemDrive
set CLOVER_HOME=%ProgramFiles%\cloverETL
goto checkJava

:checkSystemDrive
REM check for clover in root directory of system drive
if not exist %SystemDrive%\cloverETL\lib\cloveretl.engine.jar goto checkCDrive
set CLOVER_HOME=%SystemDrive%\cloverETL
goto checkJava

:checkCDrive
REM check for clover in C:\cloverETL for Win9X users
if not exist C:\cloverETL\lib\cloveretl.engine.jar goto noCloverHome
set CLOVER_HOME=C:\cloverETL
goto checkJava

:noCloverHome
echo CLOVER_HOME is set incorrectly or clover could not be located. Please set CLOVER_HOME.
goto end

:checkJava
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
set LOG4J_JAR=%CLOVER_HOME%\lib\log4j-1.2.12.zip
set POI_JAR=%CLOVER_HOME%\lib\poi-2.5.1.jar

"%_JAVACMD%" %CLOVER_OPTS% -classpath "%CLOVER_HOME%\lib\cloveretl.engine.jar;%COMMONS_LOGGING_JAR%;%JAVOLUTION_JAR%;%JMS_JAR%;%JXL_JAR%;%LOG4J_JAR%;%POI_JAR%;%TOOLS_JAR%;" "-Dclover.home=%CLOVER_HOME%" org.jetel.main.runGraph -plugins "%CLOVER_HOME%\plugins" %CLOVER_CMD_LINE_ARGS%

set CLOVER_CMD_LINE_ARGS=
set _JAVACMD=
set TOOLS_JAR=
set TRANSFORM_PATH=
set COMMONS_LOGGING_JAR=
set JAVOLUTION_JAR=
set JMS_JAR=
set JXL_JAR=
set LOG4J_JAR=
set POI_JAR=
