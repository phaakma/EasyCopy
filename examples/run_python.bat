@echo off
cls

SET python_path=C:\ArcGIS\Server\framework\runtime\ArcGIS\bin\Python\envs\arcgispro-py3\pythonw.exe
SET python_file=%1

ECHO ========================================================
ECHO SCRIPT PATH = %CD%
ECHO.
ECHO python_path = %python_path%
ECHO python_file = %python_file%
ECHO ========================================================
ECHO.

:: This line runs the python executable with the python file and outputs both stdout and stderr
:: to a log file.
:: The log file is stored in a subfolder of the current directory called "logs". It is assumed
:: that this folder already exists the batch file will throw an error if it doesn't.

"%python_path%" "%CD%\%python_file%" 1> "%CD%\logs\%python_file%_last_run.log" 2>&1
