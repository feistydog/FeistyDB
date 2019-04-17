#!/bin/sh

curl -O https://sqlite.org/2019/sqlite-amalgamation-3280000.zip
unzip -ju sqlite-amalgamation-3280000.zip -x "*/shell.c" -d sqlite
