#!/bin/sh

curl -O https://sqlite.org/2017/sqlite-amalgamation-3190300.zip
unzip -ju sqlite-amalgamation-3190300.zip -x "*/shell.c" -d sqlite
