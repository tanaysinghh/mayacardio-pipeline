@echo off
setlocal
set MODE=
set SEED=
set DURATION=
set PORT=
:parse
if "%~1"=="" goto done
if "%~1"=="--mode" (set MODE=%~2& shift & shift & goto parse)
if "%~1"=="--seed" (set SEED=%~2& shift & shift & goto parse)
if "%~1"=="--duration" (set DURATION=%~2& shift & shift & goto parse)
if "%~1"=="--port" (set PORT=%~2& shift & shift & goto parse)
goto usage
:done
if "%MODE%"=="" goto usage
if "%SEED%"=="" goto usage
if "%DURATION%"=="" goto usage
if "%PORT%"=="" goto usage
if not "%MODE%"=="signal" if not "%MODE%"=="ramp" goto usage
set JAR=%BIOSIGNAL_MOCK_JAR%
if "%JAR%"=="" set JAR=%~dp0..\dist\biosignal-mock-standin.jar
if not exist "%JAR%" set JAR=%~dp0..\mock-standin\target\biosignal-mock-standin.jar
if not exist "%JAR%" (echo biosignal-mock-standin.jar not found; build with mvnw -DskipTests package 1>&2 & exit /b 1)
set /a CTL=%PORT%+1
if not "%BIOSIGNAL_MOCK_CONTROL_PORT%"=="" set CTL=%BIOSIGNAL_MOCK_CONTROL_PORT%
set GT=%BIOSIGNAL_MOCK_GT_DIR%
if "%GT%"=="" set GT=mock-ground-truth
set SPEED=%BIOSIGNAL_MOCK_SPEED%
if "%SPEED%"=="" set SPEED=1
java -Xmx128m -XX:+UseSerialGC -jar "%JAR%" --mode %MODE% --seed %SEED% --duration %DURATION% --port %PORT% --control-port %CTL% --ground-truth-dir "%GT%" --speed %SPEED%
exit /b %ERRORLEVEL%
:usage
echo usage: biosignal-mock --mode signal^|ramp --seed N --duration SECONDS --port N 1>&2
echo STAND-IN for the official biosignal-mock. Extra settings via BIOSIGNAL_MOCK_CONTROL_PORT, BIOSIGNAL_MOCK_GT_DIR, BIOSIGNAL_MOCK_SPEED, BIOSIGNAL_MOCK_JAR 1>&2
exit /b 2
