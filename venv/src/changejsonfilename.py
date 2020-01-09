import os
from multiprocessing import Pool
import tqdm


def multi_run_wrapper(args):
    return processfiles(*args)


def processfiles(filename, rootpath, s3_key):
    os.system(
        "mv " + rootpath + filename + " " + rootpath + s3_key + ".json"
    )
    return True

def changefilenames(combinedresult_s3key, regressionType, jsonfilenameEnum, poolsize, FOLDER_PATH, threadsize):
    input = []
    for index, row in combinedresult_s3key.iterrows():
        filename = "trip." + row["driver_id"] + "." + row["trip_id"] + ".json"
        rootpath = FOLDER_PATH + "jsonfiles/" + regressionType.value + "/" + jsonfilenameEnum.value + "/" + poolsize.value + "/" + \
                   row["driver_id"] + "/"
        input.append(
            tuple((filename, rootpath, row["s3_key"])))
    pool = Pool(threadsize)
    try:
        with pool as p:
            print("Pool-size:", len(input))
            result = list(tqdm.tqdm(p.imap(multi_run_wrapper, input), total=len(input)))
    except Exception as e:
        print(e)
        pool.terminate()
        pool.join()
        exit()
