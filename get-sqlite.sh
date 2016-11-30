#!/bin/sh

curl -O https://sqlite.org/2016/sqlite-amalgamation-3150200.zip
unzip -ju sqlite-amalgamation-3150200.zip -x "*/shell.c" -d sqlite
