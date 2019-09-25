import math
import pandas as pd


def averageGeolocation(coords):
    if len(coords) == 1:
        return coords[0]
    x = 0.0
    y = 0.0
    z = 0.0

    for coord in coords:
        latitude = coord[0] * math.pi / 180
        longitude = coord[1] * math.pi / 180
        x += math.cos(latitude) * math.cos(longitude)
        y += math.cos(latitude) * math.sin(longitude);
        z += math.sin(latitude)
    total = len(coords)
    x = x / total
    y = y / total
    z = z / total

    centralLongitude = math.atan2(y, x)
    centralSquareRoot = math.sqrt(x * x + y * y)
    centralLatitude = math.atan2(z, centralSquareRoot)

    return (centralLatitude * 180 / math.pi, centralLongitude * 180 / math.pi)


# print(averageGeolocation([(37.797749, -122.412147), (37.789068, -122.390604), (37.785269, -122.421975)]))

# print(averageGeolocation([(37.928969, 138.979637), (39.029788, -119.594585), (-39.298237, 175.717917)]))

PATH = "/Users/omerorhan/Documents/EventDetection/csv/"

# datasetsnapped = pd.read_csv(PATH + "snappedLocationList.csv", index_col=False)
# print(datasetsnapped)
# datasetraw = pd.read_csv(PATH + "rawLocationList.csv", index_col=False)

# print(datasetraw.head())

from math import radians, degrees, sin, cos, asin, acos, sqrt


def great_circle(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    return 6371 * (
        acos(sin(lat1) * sin(lat2) + cos(lat1) * cos(lat2) * cos(lon1 - lon2))
    )


def distance(lon1, lat1, lon2, lat2):
    from math import sin, cos, sqrt, atan2, radians

    # approximate radius of earth in km
    R = 6373.0
    lat1 = radians(lat1)
    lon1 = radians(lon1)
    lat2 = radians(lat2)
    lon2 = radians(lon2)

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c
    return distance * 1000

def calculatedistancestopped(speedzerodf):
    rawloca = []
    snaploca = []
    for index, row in speedzerodf.iterrows():
        rawloca.append((row['rawlat'], row['rawlon']))
        snaploca.append((row['snaplat'], row['snaplon']))
    mergerawlat, mergerawlon = averageGeolocation(rawloca)
    mergesnaplat, mergesnaplon = averageGeolocation(snaploca)
    print(distance(mergerawlat, mergerawlon, mergesnaplat, mergerawlon))
    print("start="+str(rawloca[0][0]))
    print("end="+str(rawloca[len(rawloca)-1][0]))

# print(great_circle(-121.96847, 37.35153, 	-121.9683762,37.351532))
# print(distance(-121.96847, 37.35153, 	-121.9683762,37.351532))

datasetrawsnapped = pd.read_csv(PATH + "snappedrawLocationmerged.csv", index_col=False)

distlist = []
distlist.append("distance")
datasetrawsnapped["distance"] = ""
speedzerodf = pd.DataFrame(columns=['rawlat', 'rawlon', 'snaplat', 'snaplon', 'speed'])

start = False

for index, row in datasetrawsnapped.iterrows():
    if row["speed"] == 0:
        start = True
        speedzerodf = speedzerodf.append(row)
    if start and row["speed"] > 0:
        calculatedistancestopped(speedzerodf)
        start= False
    distan = round(distance(float(row['rawlon']), float(row['rawlat']), float(row['snaplon']), float(row['snaplat'])),
                   2)
    datasetrawsnapped["distance"][index] = distan
    distlist.append(distan)
#print(speedzerodf)
datasetrawsnapped.to_csv(PATH + "snappedrawLocationmerged_distance.csv")

