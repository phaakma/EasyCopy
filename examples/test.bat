@echo off
cls

SET python_path=C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe
SET python_file=test.py

ECHO ========================================================
ECHO SCRIPT PATH = %CD%
ECHO.
ECHO python_path = %python_path%
ECHO python_file = %python_file%
ECHO ========================================================
ECHO.

:: PAUSE

"%python_path%" "%CD%\%python_file%"