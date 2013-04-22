@echo off

REM this script was inspired by practices gained from ant run scripts (http://ant.apache.org/)

REM usage 
REM DBFAnalyzer.bat [-v(erbose)] <DBF filename> [<metadata output filename>]

REM prepare DERIVED_CLOVER_HOME environmental variable
REM %~dp0 is expanded pathname of the current script under NT
set "DEFAULT_CLOVER_HOME=%~dp0.."

if "%CLOVER_HOME%"=="" (set "DERIVED_CLOVER_HOME=%DEFAULT_CLOVER_HOME%") else (set "DERIVED_CLOVER_HOME=%CLOVER_HOME%")
set DEFAULT_CLOVER_HOME=

REM find CLOVER_HOME if it does not exist due to either an invalid value passed
REM by the user or the %0 problem on Windows 9x
if exist "%DERIVED_CLOVER_HOME%\lib\cloveretl.engine.jar" goto checkJava

REM check for clover in Program Files
if not exist "%ProgramFiles%\cloverETL" goto checkSystemDrive
set "DERIVED_CLOVER_HOME=%ProgramFiles%\cloverETL"
goto checkJava

:checkSystemDrive
REM check for clover in root directory of system drive
if not exist %SystemDrive%\cloverETL\lib\cloveretl.engine.jar goto checkCDrive
set "DERIVED_CLOVER_HOME=%SystemDrive%\cloverETL"
goto checkJava

:checkCDrive
REM check for clover in C:\cloverETL for Win9X users
if not exist C:\cloverETL\lib\cloveretl.engine.jar goto noCloverHome
set "DERIVED_CLOVER_HOME=C:\cloverETL"
goto checkJava

:noCloverHome
echo CLOVER_HOME is set incorrectly or clover could not be located. Please set CLOVER_HOME.
goto end

:checkJava
set "JAVA_CMD=%JAVACMD%"
if "%JAVA_HOME%" == "" goto noJavaHome
if not exist "%JAVA_HOME%\bin\java.exe" goto noJavaHome
if "%JAVA_CMD%" == "" set "JAVA_CMD=%JAVA_HOME%\bin\java.exe"
goto parseArgs

:noJavaHome
if "%JAVA_CMD%" == "" set "JAVA_CMD=java.exe"

REM Prepare the command line arguments. This loop allows for an unlimited number
REM of arguments (up to the command line limit, anyway).

:parseArgs
if ""%1""=="""" goto createClasspath
set "CMD_LINE_ARGS=%1"
shift

:cmdLineArgs
if ""%1""=="""" goto createClasspath
set "CMD_LINE_ARGS=%CMD_LINE_ARGS% %1"
shift
goto cmdLineArgs

:createClasspath
FOR /f "tokens=*" %%G IN ('dir /b "%DERIVED_CLOVER_HOME%/lib"') DO (call :collectClasspath %%G)
GOTO DBFAnalyzer

:collectClasspath
 set "ENGINE_CLASSPATH=%ENGINE_CLASSPATH%;%DERIVED_CLOVER_HOME%/lib/%1"
GOTO :eof

:DBFAnalyzer

echo "%JAVA_CMD%" -classpath "%ENGINE_CLASSPATH%" org.jetel.database.dbf.DBFAnalyzer %CMD_LINE_ARGS%
"%JAVA_CMD%" -classpath "%ENGINE_CLASSPATH%" org.jetel.database.dbf.DBFAnalyzer %CMD_LINE_ARGS%
set RETURN_CODE=%ERRORLEVEL%

set JAVA_CMD=
set CMD_LINE_ARGS=
set ENGINE_CLASSPATH=
set DERIVED_CLOVER_HOME=

exit /B %RETURN_CODE%

:end
