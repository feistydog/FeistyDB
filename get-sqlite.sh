#!/bin/sh

curl -O https://sqlite.org/2020/sqlite-amalgamation-3310000.zip
unzip -ju sqlite-amalgamation-3310000.zip -x "*/shell.c" -d sqlite
