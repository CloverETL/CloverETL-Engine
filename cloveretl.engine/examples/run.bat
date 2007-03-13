@echo off

rem ********************************
rem usage - run.bat <graph_name.grf>
rem ********************************

rem ***** set this before using script *****
set ENGINE_PATH=..
set TRANSFORM_PATH=javaExamples
rem ****************************************

set ENGINE_JAR=%ENGINE_PATH%\cloveretl.engine.jar
set COMMONS_LOGGING_JAR=%ENGINE_PATH%\lib\commons-logging.jar
set JAVOLUTION_JAR=%ENGINE_PATH%\lib\javolution.jar
set JMS_JAR=%ENGINE_PATH%\lib\jms.jar
set JXL_JAR=%ENGINE_PATH%\lib\jxl.jar
set LOG4J_JAR=%ENGINE_PATH%\lib\log4j-1.2.12.zip
set POI_JAR=%ENGINE_PATH%\lib\poi-2.5.1.jar

java -cp "%TRANSFORM_PATH%;%ENGINE_JAR%;%COMMONS_LOGGING_JAR%;%JAVOLUTION_JAR%;%JMS_JAR%;%JXL_JAR%;%LOG4J_JAR%;%POI_JAR%;%JAVA_HOME%\lib\tools.jar" org.jetel.main.runGraph -plugins %ENGINE_PATH%\plugins %1
