@echo off
cd ServerTest\bin\debug\netcoreapp2.2
start dotnet ServerTest.dll
TIMEOUT 5 > NUL
cd ..\..\..\..

cd ClientTest\bin\debug\netcoreapp2.2
TIMEOUT 1 > NUL
start dotnet ClientTest.dll
TIMEOUT 1 > NUL
start dotnet ClientTest.dll
cd ..\..\..\..
@echo on
