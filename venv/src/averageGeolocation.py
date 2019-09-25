import math
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
    x = x / total;
    y = y / total;
    z = z / total;

    centralLongitude = math.atan2(y, x)
    centralSquareRoot = math.sqrt(x * x + y * y)
    centralLatitude = math.atan2(z, centralSquareRoot)

    return (centralLatitude * 180 / math.pi,centralLongitude * 180 /math.pi)

print(averageGeolocation([(37.797749, -122.412147), (37.789068, -122.390604), (37.785269, -122.421975)]))

print(averageGeolocation([(37.928969, 138.979637), (39.029788, -119.594585), (-39.298237, 175.717917)]))