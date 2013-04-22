REM this script was inspired by practices gained from ant run scripts (http://ant.apache.org/)

REM split command-line arguments to two sets - clover and jvm arguments
REM and define DERIVED_CLOVER_HOME variable


REM prepare DERIVED_CLOVER_HOME environmental variable
REM %~dp0 is expanded pathname of the current script under NT
set "DEFAULT_CLOVER_HOME=%~dp0.."

if "%CLOVER_HOME%"=="" (set "DERIVED_CLOVER_HOME=%DEFAULT_CLOVER_HOME%") else (set "DERIVED_CLOVER_HOME=%CLOVER_HOME%")
set DEFAULT_CLOVER_HOME=


REM Prepare the command line arguments. This loop allows for an unlimited number
REM of arguments (up to the command line limit, anyway).
REM all arguments before "-" are stored in CLOVER_CMD_LINE_ARGS
REM and the others are stored in JAVA_CMD_LINE_ARGS
set "CLOVER_CMD_LINE_ARGS=%1"
if ""%1""=="""" goto doneStart
if ""%1""==""-"" goto javaArgs
shift
:setupArgs
if ""%1""=="""" goto doneStart
if ""%1""==""-"" goto javaArgs
set "CLOVER_CMD_LINE_ARGS=%CLOVER_CMD_LINE_ARGS% %1"
shift
goto setupArgs

:javaArgs
shift
if ""%1""==""-classpath"" (
	set WAS_CLASSPATH=yes
	goto nextJavaArgs
)
set "JAVA_CMD_LINE_ARGS=%1"
if ""%1""=="""" goto doneStart
:nextJavaArgs
shift
if ""%1""=="""" goto doneStart
if ""%1""==""-classpath"" (
	set WAS_CLASSPATH=yes
	goto nextJavaArgs
)
if ""%WAS_CLASSPATH%""==""yes"" (
	set "USER_CLASSPATH=%1"
	set WAS_CLASSPATH=
	goto nextJavaArgs
)
set "JAVA_CMD_LINE_ARGS=%JAVA_CMD_LINE_ARGS% %1"
goto nextJavaArgs

:doneStart
REM find CLOVER_HOME if it does not exist due to either an invalid value passed
REM by the user or the %0 problem on Windows 9x
if exist "%DERIVED_CLOVER_HOME%\lib\cloveretl.engine.jar" goto end
echo Clover installation directory does not found at %DERIVED_CLOVER_HOME% 

REM check for clover in Program Files
if not exist "%ProgramFiles%\cloverETL" goto checkSystemDrive
set "DERIVED_CLOVER_HOME=%ProgramFiles%\cloverETL"
echo Clover installation directory found at %DERIVED_CLOVER_HOME%
goto end

:checkSystemDrive
REM check for clover in root directory of system drive
if not exist %SystemDrive%\cloverETL\lib\cloveretl.engine.jar goto checkCDrive
set "DERIVED_CLOVER_HOME=%SystemDrive%\cloverETL"
echo Clover installation directory found at %DERIVED_CLOVER_HOME%
goto end

:checkCDrive
REM check for clover in C:\cloverETL for Win9X users
if not exist C:\cloverETL\lib\cloveretl.engine.jar goto noCloverHome
set "DERIVED_CLOVER_HOME=C:\cloverETL"
echo Clover installation directory found at %DERIVED_CLOVER_HOME%
goto end

:noCloverHome
echo CLOVER_HOME is set incorrectly or clover could not be located. Please set CLOVER_HOME.
exit

:end
echo CLOVER_HOME=%DERIVED_CLOVER_HOME%
echo CLOVER_CMD_LINE_ARGS=%CLOVER_CMD_LINE_ARGS%
echo JAVA_CMD_LINE_ARGS=%JAVA_CMD_LINE_ARGS%
echo USER_CLASSPATH=%USER_CLASSPATH%
