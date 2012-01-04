@echo off

REM this script was inspired by practices gained from ant run scripts (http://ant.apache.org/)

REM usage
REM analyzeDB.bat <analyze_arguments>
REM example:
REM analyzeDB.bat -config interbase.cfg -o employee.fmt -q "select * from EMPLOYEE"

REM prepare CLOVER_HOME environmental variable
REM %~dp0 is expanded pathname of the current script under NT
set DEFAULT_CLOVER_HOME=%~dp0..
if "%CLOVER_HOME%"=="" set CLOVER_HOME=%DEFAULT_CLOVER_HOME%
set DEFAULT_CLOVER_HOME=

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
set JAVA_CMD=%JAVACMD%
if "%JAVA_HOME%" == "" goto noJavaHome
if not exist "%JAVA_HOME%\bin\java.exe" goto noJavaHome
if "%JAVA_CMD%" == "" set JAVA_CMD=%JAVA_HOME%\bin\java.exe
goto analyzeDB

:noJavaHome
if "%JAVA_CMD%" == "" set JAVA_CMD=java.exe

REM Prepare the command line arguments. This loop allows for an unlimited number
REM of arguments (up to the command line limit, anyway).

if ""%1""=="""" goto analyzeDB
set CMD_LINE_ARGS=%1
shift

:cmdLineArgs
if ""%1""=="""" goto analyzeDB
set CMD_LINE_ARGS=%CMD_LINE_ARGS% %1
shift
goto cmdLineArgs

:analyzeDB

set CLOVER_ENGINE_JAR=%CLOVER_HOME%\lib\cloveretl.engine.jar
set CLOVER_CONNECTION_JAR=%CLOVER_HOME%\plugins\org.jetel.connection\cloveretl.connection.jar
set COMMONS_LOGGING_JAR=%CLOVER_HOME%\lib\commons-logging-1.1.1.jar
set LOG4J_JAR=%CLOVER_HOME%\lib\log4j-1.2.15.jar

set CLOVER_PLUGINS_DIR=%CLOVER_HOME%\plugins

echo "%JAVA_CMD%" -classpath "%CLOVER_ENGINE_JAR%;%CLOVER_CONNECTION_JAR%;%COMMONS_LOGGING_JAR%;%LOG4J_JAR%" org.jetel.connection.jdbc.AnalyzeDB %CMD_LINE_ARGS% -plugins %CLOVER_PLUGINS_DIR%
"%JAVA_CMD%" -classpath "%CLOVER_ENGINE_JAR%;%CLOVER_CONNECTION_JAR%;%COMMONS_LOGGING_JAR%;%LOG4J_JAR%" org.jetel.connection.jdbc.AnalyzeDB %CMD_LINE_ARGS% -plugins %CLOVER_PLUGINS_DIR%
set RETURN_CODE=%ERRORLEVEL%

set JAVA_CMD=
set CMD_LINE_ARGS=

set CLOVER_ENGINE_JAR=
set CLOVER_CONNECTION_JAR=
set COMMONS_LOGGING_JAR=
set LOG4J_JAR=

set CLOVER_PLUGINS_DIR=

exit /B %RETURN_CODE%

:end
