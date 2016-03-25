@echo off
cd D:\Code\Misc\BigQ\BigQServerTest\bin\debug
start BigQServerTest.exe
TIMEOUT 2 > NUL

cd D:\Code\Misc\BigQ\BigQClientTest\bin\debug
start BigQClientTest.exe
start BigQClientTest.exe
cd ..\..\..
@echo on
