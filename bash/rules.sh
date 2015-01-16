#!/bin/bash

servers=( fyanucio@i-0000003c-vsm.melicloud.com fyanucio@i-00000035-ysm.melicloud.com fyanucio@i-00000035-wsm.melicloud.com fyanucio@i-0000003d-xsm.melicloud.com )

#Generating rule keys for shipping method 182
echo "Generating rule keys for shipping method 182..."
ruleKeys1_182=(`ssh ${servers[0]} /usr/local/bin/redis/redis-cli keys "RULE-*-182" 2> /dev/null`)
ruleKeys2_182=(`ssh ${servers[1]} /usr/local/bin/redis/redis-cli keys "RULE-*-182" 2> /dev/null`)
ruleKeys3_182=(`ssh ${servers[2]} /usr/local/bin/redis/redis-cli keys "RULE-*-182" 2> /dev/null`)
ruleKeys4_182=(`ssh ${servers[3]} /usr/local/bin/redis/redis-cli keys "RULE-*-182" 2> /dev/null`)

ruleKeys_182=( ${ruleKeys1_182[*]} ${ruleKeys2_182[*]} ${ruleKeys3_182[*]} ${ruleKeys4_182[*]} )
echo "done."

#Generating rule values for shipping method 182
echo "Generating rule values for shipping method 182...(it takes a little longer)"
ruleValues1_182=(`ssh ${servers[0]} /usr/local/bin/redis/redis-cli mget ${ruleKeys1_182[*]} 2> /dev/null | tr ' ' '*'`)
ruleValues2_182=(`ssh ${servers[1]} /usr/local/bin/redis/redis-cli mget ${ruleKeys2_182[*]} 2> /dev/null | tr ' ' '*'`)
ruleValues3_182=(`ssh ${servers[2]} /usr/local/bin/redis/redis-cli mget ${ruleKeys3_182[*]} 2> /dev/null | tr ' ' '*'`)
ruleValues4_182=(`ssh ${servers[3]} /usr/local/bin/redis/redis-cli mget ${ruleKeys4_182[*]} 2> /dev/null | tr ' ' '*'`)

ruleValues_182=( ${ruleValues1_182[*]} ${ruleValues2_182[*]} ${ruleValues3_182[*]} ${ruleValues4_182[*]} )
echo "done."

#Generating rules for shipping method 100009
echo "Generating rule keys for shipping method 100009..."
ruleKeys1_100009=(`ssh ${servers[0]} /usr/local/bin/redis/redis-cli keys "RULE-*-100009" 2> /dev/null`)
ruleKeys2_100009=(`ssh ${servers[1]} /usr/local/bin/redis/redis-cli keys "RULE-*-100009" 2> /dev/null`)
ruleKeys3_100009=(`ssh ${servers[2]} /usr/local/bin/redis/redis-cli keys "RULE-*-100009" 2> /dev/null`)
ruleKeys4_100009=(`ssh ${servers[3]} /usr/local/bin/redis/redis-cli keys "RULE-*-100009" 2> /dev/null`)

ruleKeys_100009=( ${ruleKeys1_100009[*]} ${ruleKeys2_100009[*]} ${ruleKeys3_100009[*]} ${ruleKeys4_100009[*]} )
echo "done."

#Generating rule values for shipping method 100009
echo "Generating rule values for shipping method 100009...(it takes a little longer)"
ruleValues1_100009=(`ssh ${servers[0]} /usr/local/bin/redis/redis-cli mget ${ruleKeys1_100009[*]} 2> /dev/null | tr ' ' '*'`)
ruleValues2_100009=(`ssh ${servers[1]} /usr/local/bin/redis/redis-cli mget ${ruleKeys2_100009[*]} 2> /dev/null | tr ' ' '*'`)
ruleValues3_100009=(`ssh ${servers[2]} /usr/local/bin/redis/redis-cli mget ${ruleKeys3_100009[*]} 2> /dev/null | tr ' ' '*'`)
ruleValues4_100009=(`ssh ${servers[3]} /usr/local/bin/redis/redis-cli mget ${ruleKeys4_100009[*]} 2> /dev/null | tr ' ' '*'`)

ruleValues_100009=( ${ruleValues1_100009[*]} ${ruleValues2_100009[*]} ${ruleValues3_100009[*]} ${ruleValues4_100009[*]} )
echo "done."


#Validating lengths
echo "Validating lengths..."
len182=${#ruleKeys_182[*]}
len100009=${#ruleKeys_100009[*]}
lenValues182=${#ruleValues_182[*]}
lenValues100009=${#ruleValues_100009[*]}
if [ $len182 != $lenValues182 ]; then
	echo "ERROR: different quantities of keys and values in rules 182"
fi 
if [ $len100009 != $lenValues100009 ]; then
	echo "ERROR: different quantities of keys and values in rules 100009"
fi 
echo "Total rules (182)    = $len182"
echo "Total rules (100009) = $len100009"
if [ "$len182" != "$len100009" ]; then
	echo "WARNING: different size in rules"
fi
echo "done."


read -p "Press [Enter] key to continue"

#Validating existence of rules and its content
echo "Validating existence of rules and its content..."

echo "Processing rules 182"
processed182=0
for (( i=0; i<$len182; i++ )); do
	ruleKey182=${ruleKeys_182[i]}
	found=false
	for (( j=0; j<$len100009; j++ )); do
		ruleKey100009=${ruleKeys_100009[j]}
			
		to182=$((${#ruleKey182} - 4))
		commonKey182=${ruleKey182:0:to182}
		to100009=$((${#ruleKey100009} - 7))
		commonKey100009=${ruleKey100009:0:to100009}
		
		if [ $commonKey182 == $commonKey100009 ]; then
			found=true
			
			#Compare contents
			content182=${ruleValues_182[i]}
			content100009=${ruleValues_100009[j]} 
			if [ $content182 == "NO_RULE" ] && [ $content100009 != "NO_RULE" ] || [ $content182 != "NO_RULE" ] && [ $content100009 == "NO_RULE" ]; then
				echo "ERROR: Rule $commonKey182 has different content"
			fi
			
			break
		fi
	done
	if [ $found == false ]; then
		echo "ERROR: Rule $commonKey182 not found in 100009 rules"
	fi
	processed182=$(($processed182 + 1))
	mod100=$(( $processed182 % 100 ))
	if [ $mod100 == 0 ]; then
		echo "$processed182 completed"
	fi
done

echo "Processing rules 100009"
processed100009=0
for (( i=0; i<$len100009; i++ )); do
	ruleKey100009=${ruleKeys_100009[i]}
	found=false
	for (( j=0; j<$len182; j++ )); do
		ruleKey182=${ruleKeys_182[j]}
		
		to100009=$((${#ruleKey100009} - 7))
		commonKey100009=${ruleKey100009:0:to100009}
		to182=$((${#ruleKey182} - 4))
		commonKey182=${ruleKey182:0:to182}
				
		if [ $commonKey100009 == $commonKey182 ]; then
			found=true
			
			#Compare contents
			content100009=${ruleValues_100009[i]} 
			content182=${ruleValues_182[j]}
			if [ $content100009 == "NO_RULE" ] && [ $content182 != "NO_RULE" ] || [ $content100009 != "NO_RULE" ] && [ $content182 == "NO_RULE" ]; then
				echo "ERROR: Rule $commonKey100009 has different content"
			fi
			
			break
		fi
	done
	if [ $found == false ]; then
		echo "Rule $commonKey100009 not found in 182 rules"
	fi
	processed100009=$(($processed100009 + 1))
	mod100=$(( $processed100009 % 100 ))
	if [ $mod100 == 0 ]; then
		echo "$processed100009 completed"
	fi
done

echo "done."
echo "exiting..."
