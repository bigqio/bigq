@echo off
cd ServerTestNetFramework\bin\debug
start BigQServerTest.exe
TIMEOUT 1 > NUL
cd ..\..\..

cd ClientTestNetFramework\bin\debug
start BigQClientTest.exe
start BigQClientTest.exe
cd ..\..\..
@echo on
