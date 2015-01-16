#!/bin/bash

#Validating arguments
arg1=$1
arg2=$2

#Redis servers
servers=( e-000011c9.melicloud.com e-000011cb.melicloud.com e-000011cc.melicloud.com )

#Usage
if [ -z $arg1 ] && [ -z $arg2 ]; then
	echo "Usage: ./handlingTime.sh userId  OR  ./handlingTime.sh [eq|gt|lt] value"
	exit 0
fi

#Validating arguments
if [ -z $arg2 ]; then
	#One argument is for searching HT for one user
	userId=$1
	if  ! [[ "$userId" =~ ^[0-9]+$ ]] || [ $userId -lt 0 ]; then
		echo "Usage: ./handlingTime.sh userId"
		echo "'userId' must be an integer number"
		exit 1
	fi

	userValue1=`ssh ${servers[0]} /usr/local/bin/redis/src/redis-cli mget ${userId} 2> /dev/null`
	userValue2=`ssh ${servers[1]} /usr/local/bin/redis/src/redis-cli mget ${userId} 2> /dev/null`
	userValue3=`ssh ${servers[2]} /usr/local/bin/redis/src/redis-cli mget ${userId} 2> /dev/null`

	if [ -z $userValue1 ] && [ -z $userValue2 ] && [ -z $userValue3 ]; then
		echo "User not found"
	else
		if [[ $userValue1 ]]; then
			echo "HT: $userValue1"
		else 
			if [[ $userValue2 ]]; then
				echo "HT: $userValue2"
			else
				echo "HT: $userValue3"
			fi
		fi
	fi

else

	#Two arguments is for getting list of HT according to specific value
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

	#Generating keys
	echo "Generating keys for all users..."
	userKeys1=(`ssh ${servers[0]} '/usr/local/bin/redis/src/redis-cli keys "*"' 2> /dev/null`)
	userKeys2=(`ssh ${servers[1]} '/usr/local/bin/redis/src/redis-cli keys "*"' 2> /dev/null`)
	userKeys3=(`ssh ${servers[2]} '/usr/local/bin/redis/src/redis-cli keys "*"' 2> /dev/null`)

	userKeys=( ${userKeys1[*]} ${userKeys2[*]} ${userKeys3[*]} )
	echo "done."

	#Generating values
	echo "Generating values for all users..."
	userValues1=(`ssh ${servers[0]} /usr/local/bin/redis/src/redis-cli mget ${userKeys1[*]} 2> /dev/null | tr ' ' '*'`)
	userValues2=(`ssh ${servers[1]} /usr/local/bin/redis/src/redis-cli mget ${userKeys2[*]} 2> /dev/null | tr ' ' '*'`)
	userValues3=(`ssh ${servers[2]} /usr/local/bin/redis/src/redis-cli mget ${userKeys3[*]} 2> /dev/null | tr ' ' '*'`)

	userValues=( ${userValues1[*]} ${userValues2[*]} ${userValues3[*]} )
	echo "done."

	#Validating lengths
	echo "Validating lengths..."
	lenKeys=${#userKeys[*]}
	lenValues=${#userValues[*]}
	if [ $lenKeys != $lenValues ]; then
		echo "ERROR: different quantities of keys and values"
	fi 
	echo "done."

	echo "Processing..."
	resultFound=false
	for (( i=0; i<$lenValues; i++ )); do
		if [ ${userValues[i]:0:($((${#userValues[i]} - 2)))} -$comparator "$valueToCompare" ]; then
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

fi


