@echo off
IF [%1] == [] GOTO Usage
cd BigQServerTest\bin\debug
start BigQServerTest.exe
TIMEOUT 5 > NUL
cd ..\..\..

cd BigQClientTest\bin\debug
FOR /L %%i IN (1,1,%1) DO (
ECHO Starting client %%i
start BigQClientTest.exe
TIMEOUT 1 > NUL
)
cd ..\..\..
@echo on
EXIT /b

:Usage
ECHO Specify the number of client nodes to start.
@echo on
EXIT /b
