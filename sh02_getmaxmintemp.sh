#!/usr/bin/bash

# This script was generated to separate the site files 

# Get site list

# Step 2: get unique dates in each
for tempfile in *.AllAirTemp04to1610mins; do
	awk '{print $2}' $tempfile | uniq > $tempfile.dates
	echo $tempfile
	echo > $tempfile.maxmin
	cat $tempfile.dates | while read tdline; do
		maximum=`grep -w "$tdline" $tempfile | awk 'BEGIN {max=-100} {if ($4 > max) max=$4} END {print max}'`
		minimum=`grep -w "$tdline" $tempfile | awk 'BEGIN {min=100} {if ($4 < min) min=$4} END {print min}'`
		
		echo "$tdline	$maximum	$minimum" >> $tempfile.maxmin
	done
done

