from os import walk
import os
from shutil import copyfile
#source_folder_path = "/Users/omerorhan/Documents/EventDetection/regression_server/raw/"
source_folder_path = "/home/ec2-user/omer/dataForRegressionTestRaw/"
#destination_folder_path = "/Users/omerorhan/Documents/EventDetection/regression_server/raw/tripfiles/"
destination_folder_path = "/home/ec2-user/omer/tripfiles/"
f = []
processcount = 0
for (dirpath, dirnames, filenames) in walk(source_folder_path):
    for filename in filenames:
        if filename.endswith('.bin_v2.gz') and filename.startswith('trip'):
            splitlist = filename.split(".")
            if len(splitlist) == 5:
                driverid = splitlist[1]
                if not os.path.exists(destination_folder_path + driverid):
                    os.mkdir(destination_folder_path + driverid)
                copyfile(source_folder_path + filename,
                         destination_folder_path + driverid + "/" + filename)
                processcount=processcount+1
                print(source_folder_path + filename +" copied to " +destination_folder_path + driverid + "/" + filename)

print(str(processcount)+ "trips added")