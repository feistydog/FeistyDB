#!/bin/sh

curl -O https://sqlite.org/2019/sqlite-amalgamation-3300100.zip
unzip -ju sqlite-amalgamation-3300100.zip -x "*/shell.c" -d sqlite
