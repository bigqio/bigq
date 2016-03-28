@echo off
cd BigQServerTest\bin\release
start BigQServerTest.exe
TIMEOUT 1 > NUL
cd ..\..\..

cd BigQClientTest\bin\release
start BigQClientTest.exe
start BigQClientTest.exe
cd ..\..\..
@echo on
