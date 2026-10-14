@echo off
setlocal
set HERE=%~dp0
if "%~2"=="" goto usage
if not "%~3"=="" goto usage
set JAR=%PIPELINE_TOOLS_JAR%
if "%JAR%"=="" set JAR=%HERE%..\dist\pipeline-tools.jar
if not exist "%JAR%" set JAR=%HERE%..\tools\target\pipeline-tools.jar
if not exist "%JAR%" (echo pipeline-tools.jar not found; build with mvnw -DskipTests package 1>&2 & exit /b 1)
java -Xmx512m -jar "%JAR%" check-standin --export "%~1" --gt "%~2"
exit /b %ERRORLEVEL%
:usage
echo usage: check_ground_truth EXPORT.jsonl GROUND_TRUTH.json 1>&2
echo STAND-IN for the official check_ground_truth. Exit code 0 = pass, 1 = fail. 1>&2
exit /b 2
