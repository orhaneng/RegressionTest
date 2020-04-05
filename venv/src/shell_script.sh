#!/bin/bash  
set -e  
count=1
for x in $s3list
do
    printf $count
	count=`expr $count + 1`
  	aws s3 cp s3://mentor.trips.production-365/$x /home/ec2-user/regressiontest/tripfiles/tlm112/$x
done