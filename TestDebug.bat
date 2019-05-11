@echo off
cd ServerTest\bin\debug\net462
start ServerTest.exe
TIMEOUT 1 > NUL
cd ..\..\..\..

cd ClientTest\bin\debug\net462
start ClientTest.exe
start ClientTest.exe
cd ..\..\..\..
@echo on
