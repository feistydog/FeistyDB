#!/bin/sh

curl -O https://sqlite.org/2018/sqlite-amalgamation-3250300.zip
unzip -ju sqlite-amalgamation-3250300.zip -x "*/shell.c" -d sqlite
