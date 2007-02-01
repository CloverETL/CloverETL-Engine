@echo off

rem ********************************
rem usage - run.bat <graph_name.grf>
rem ********************************

rem ***** set this before using script *****
set JAVA_HOME=c:\Program Files (x86)\Java\jdk1.5.0_04
set ENGINE_PATH=.
set LIB_PATH=.\lib
set TRANSFORM_PATH=.
rem ****************************************

set ENGINE_JAR=%ENGINE_PATH%\cloveretl.engine.jar
set COMMONS_LOGGING_JAR=%LIB_PATH%\commons-logging.jar
set JAVOLUTION_JAR=%LIB_PATH%\javolution.jar
set JMS_JAR=%LIB_PATH%\jms.jar
set JXL_JAR=%LIB_PATH%\jxl.jar
set LOG4J_JAR=%LIB_PATH%\log4j-1.2.12.zip
set POI_JAR=%LIB_PATH%\poi-2.5.1.jar

java -cp "%TRANSFORM_PATH%;%ENGINE_JAR%;%COMMONS_LOGGING_JAR%;%JAVOLUTION_JAR%;%JMS_JAR%;%JXL_JAR%;%LOG4J_JAR%;%POI_JAR%;%JAVA_HOME%\lib\tools.jar" org.jetel.main.runGraph %1
