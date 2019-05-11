@echo off
IF [%1] == [] GOTO Usage
cd ServerTest\bin\debug\net462
start ServerTest.exe
TIMEOUT 3 > NUL
cd ..\..\..\..

cd ClientTest\bin\debug\net462
FOR /L %%i IN (1,1,%1) DO (
ECHO Starting client %%i
start ClientTest.exe
TIMEOUT 3 > NUL
)
cd ..\..\..\..
@echo on
EXIT /b

:Usage
ECHO Specify the number of client nodes to start.
@echo on
EXIT /b
