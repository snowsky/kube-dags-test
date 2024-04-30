#!/usr/bin/env bash
set -ex
python -m pytest $@ -vvl --showlocals
