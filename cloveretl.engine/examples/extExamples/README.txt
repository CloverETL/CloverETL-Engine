CloverETL ExtExamples
===========================

This project contains database examples and other examples that require external dependencies.

INTRODUCTION
All database examples presented here are configured to use DERBY database which is included.
You can find libraries for derby database under lib/derby/ folder.


GETTING STARTED
In order to use the examples here, you need to start Derby database system. It is very simple
process, which does not require installation or any other dependecies


SETTING UP PROJECT FOLDER
In this project, you also need to set ${PROJECT} parameter to ABSOLUTE PATH of your project.
Please open "workspace.prm" in project folder and edit PROJECT variable accordingly.


STARTING DERBY DATABASE
- Go to lib/derby/bin
- on Windows, open Command Prompt and run "startNetworkServer.bat"
- on Linux, run "startNetworkServer"
- this will start Derby server instance bound to standard port 1527

KNOWN ISSUES on WINDOWS
Derby startup scripts do not cope well with system-wide CLASSPATH and JAVA_HOME containing quotes (") - e.g.
CLASSPATH=.;"C:\Program Files\Java\XY" - the only solution at the moment is to remove the quotes from these ENV variables


STOPPING DERBY DATABASE
- Go to lib/derby/bin
- on Windows, open Command Prompt and run "stopNetworkServer.bat" or simply close the database console window
- on Linux, run "stopNetworkServer"



