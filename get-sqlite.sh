#!/bin/sh

curl -O https://sqlite.org/2016/sqlite-amalgamation-3150100.zip
unzip -ju sqlite-amalgamation-3150100.zip -x "*/shell.c" -d sqlite
