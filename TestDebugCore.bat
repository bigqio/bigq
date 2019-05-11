@echo off
cd ServerTest\bin\debug\netcoreapp2.2
start dotnet ServerTest.dll
TIMEOUT 1 > NUL
cd ..\..\..\..

cd ClientTest\bin\debug\netcoreapp2.2
start dotnet ClientTest.dll
start dotnet ClientTest.dll
start dotnet ClientTest.dll
start dotnet ClientTest.dll
start dotnet ClientTest.dll
cd ..\..\..\..
@echo on
