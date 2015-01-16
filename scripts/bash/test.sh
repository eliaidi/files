# -----------------------------------------------------------------
# Another array, "area2".
# Another way of assigning array variables...
# array_name=( XXX YYY ZZZ ... )

area2=( zero one two three four )

echo -n "area2[0] = "
echo ${area2[0]}
# Aha, zero-based indexing (first element of array is [0], not [1]).

echo -n "area2[1] = "
echo ${area2[1]}    # [1] is second element of array.

max=1000
processed=0
echo "probando for..."
for (( i=0; i<$max; i++))
do
	processed=$(( $processed + 1 ))
	percentage=`echo $(( $processed * 100 / $max )) | bc`
	echo $percentage
	mod10=$(( $percentage % 10 ))
	if [ $percentage -ge 10 ] && [ $mod10 == 0 ]; then
		echo "$percentage% completed"
	fi
done

 

#for n in {1..5} do
	#echo ${area2[n]}
#	echo "hola"
#done
# -----------------------------------------------------------------
