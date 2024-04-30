@set /A _tic=%time:~0,2%*3600^
            +%time:~3,1%*10*60^
            +%time:~4,1%*60^
            +%time:~6,1%*10^
            +%time:~7,1% >nul

:::::::::::::

pushd %~dp0

php "TargetPopulation.php"

popd

:::::::::::::

@set /A _toc=%time:~0,2%*3600^
            +%time:~3,1%*10*60^
            +%time:~4,1%*60^
            +%time:~6,1%*10^
            +%time:~7,1% >nul

@set /A _elapsed=%_toc%-%_tic
@echo %_elapsed% seconds.


exit