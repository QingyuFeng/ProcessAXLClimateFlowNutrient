#!/usr/bin/bash

# This script was generated to separate the site files 

# Get site list

# Step 1, get the , changed to space and then to tab
sed -i "s/,/ /g" *.csv
sed -i "s/ /\t/g" *.csv

# Step 2, separate the files into single files for each station
for csvfile in *.csv; do
	[ ! -d "${csvfile%.*}"  ] && mkdir "${csvfile%.*}"
	echo "${csvfile%.*}"
	awk '{print $1}' $csvfile | uniq > "${csvfile%.*}"/"${csvfile%.*}".sitelsf
	cat "${csvfile%.*}"/"${csvfile%.*}".sitelsf | while read sites; do
		echo > "${csvfile%.*}"/$sites."${csvfile%.*}"
		grep -w "$sites" $csvfile >> "${csvfile%.*}"/$sites."${csvfile%.*}"
	done
done

# Later each individual variable will be treated separately


# Step 2: get unique dates in each
# for tfile in *.temp; do
	# awk '{print $2}' $tfile | uniq > $tfile.dates
	# echo > $tfile.maxmin
	# cat $tfile.dates | while read tdline; do
		# maximum=`grep -w "$tdline" $tfile | awk 'BEGIN {max=-100} {if ($4 > max) max=$4} END {print max}'`
		# minimum=`grep -w "$tdline" $tfile | awk 'BEGIN {min=100} {if ($4 < min) min=$4} END {print min}'`
		
		# echo "$tdline	$maximum	$minimum" >> $tfile.maxmin
	# done
# done

