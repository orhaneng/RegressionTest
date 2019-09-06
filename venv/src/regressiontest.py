import os

print("=======REGRESSION TEST==========")
FOLDER_PATH="/home/ec2-user/regressiontest/"
response = input("Did you put your build version in "+FOLDER_PATH+"/build (Y/N):")
if response.upper() != 'Y':
    if len(os.listdir(FOLDER_PATH + 'build')) > 0:
        print("current(under build folder) version will be tested!")
    else:
        print("you can not continue without build!")
        exit()


