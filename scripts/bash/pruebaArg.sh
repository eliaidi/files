#!/bin/bash

arg1=$1
echo $arg1
arg2=$2
echo $arg2

if [ -z "$arg2" ]; then
	echo "Solo 1 argumento"
else
	echo "2 argumentos"
fi

exit 0