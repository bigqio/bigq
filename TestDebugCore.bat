@echo off
cd ServerTestNetCore\bin\debug\netcoreapp2.0
start dotnet BigQServerTest.dll
TIMEOUT 1 > NUL
cd ..\..\..\..

cd ClientTestNetCore\bin\debug\netcoreapp2.0
start dotnet BigQClientTest.dll
start dotnet BigQClientTest.dll
cd ..\..\..\..
@echo on
