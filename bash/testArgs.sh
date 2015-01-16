#!/bin/bash

#Validating arguments
comparator=$1
valueToCompare=$2
if [ "$comparator" != "eq" ] && [ "$comparator" != "gt" ] && [ "$comparator" != "lt" ]; then
	echo "Usage: ./handlingTime.sh [eq|gt|lt] value"
	exit 1
fi
if  ! [[ "$valueToCompare" =~ ^[0-9]+$ ]] || [ $valueToCompare -lt 0 ]; then
	echo "Usage: ./handlingTime.sh [eq|gt|lt] value"
	echo "'value' must be an integer number >= 0"
	exit 1
fi

lenValues=7
userKeys=( 123 456 789 1011 1213 1415 1617 )
userValues=( 5 10 15 20 25 30 35 )

echo "Processing..."
resultFound=false
for (( i=0; i<$lenValues; i++ )); do
	if [ ${userValues[i]} -$comparator $valueToCompare ]; then
		echo "UserId: ${userKeys[i]} - HT: ${userValues[i]}"
		resultFound=true
	fi
done
if [ $resultFound == false ]; then
	echo "No results found"
fi
echo "done."
echo "exiting..."
exit 0