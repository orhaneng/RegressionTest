def VersionFile(file_dir, extension):
    # https://www.oreilly.com/library/view/python-cookbook/0596001673/ch04s26.html
    import os, shutil
    import glob
    for file_name in os.listdir(file_dir):
        if file_name.endswith(extension):
            lastversion = 0
            for file_namev in os.listdir(file_dir + "archive/"):
                if file_namev.startswith(file_name):
                    if len(file_namev.split(".")) > 3:
                        lastversion = max(int(file_namev.split(".")[len(file_namev.split(".")) - 1]), lastversion)
                    else:
                        lastversion = max(0, lastversion)
            shutil.move(file_dir + file_name, file_dir + "archive/" + file_name + "." + str(lastversion + 1))
