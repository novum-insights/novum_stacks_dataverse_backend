#!/usr/bin/env bash

if [ -n "$1" ]
then
  python -m unittest discover -s $1
else
  python -m unittest discover -s tests/unit/
fi
