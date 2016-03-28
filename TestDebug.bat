@echo off
cd BigQServerTest\bin\debug
start BigQServerTest.exe
TIMEOUT 1 > NUL
cd ..\..\..

cd BigQClientTest\bin\debug
start BigQClientTest.exe
start BigQClientTest.exe
cd ..\..\..
@echo on
