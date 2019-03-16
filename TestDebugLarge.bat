@echo off
IF [%1] == [] GOTO Usage
cd ServerTestNetFramework\bin\debug
start BigQServerTest.exe
TIMEOUT 3 > NUL
cd ..\..\..

cd ClientTestNetFramework\bin\debug
FOR /L %%i IN (1,1,%1) DO (
ECHO Starting client %%i
start BigQClientTest.exe
TIMEOUT 3 > NUL
)
cd ..\..\..
@echo on
EXIT /b

:Usage
ECHO Specify the number of client nodes to start.
@echo on
EXIT /b
