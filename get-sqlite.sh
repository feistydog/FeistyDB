#!/bin/sh

curl -O https://sqlite.org/2019/sqlite-amalgamation-3270200.zip
unzip -ju sqlite-amalgamation-3270200.zip -x "*/shell.c" -d sqlite
