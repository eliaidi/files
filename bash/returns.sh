#!/bin/bash

date=$1

scp "batman:/home/Desarrollo/fyanucio/newReturns${date}.csv" ~/Desktop/files/returns/
scp "batman:/home/Desarrollo/fyanucio/delayedReturns${date}.csv" ~/Desktop/files/returns/