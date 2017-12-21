def get_direct_neighbor_ids(cell, t_min, t_max, ln_min, ln_max, lt_min, lt_max, cell_xi):
    key_parts = cell.split("_")
    lat, lon, time = int(key_parts[0]), int(key_parts[1]), int(key_parts[2])
    result_tuples = []

    lat_from = lat if lt_min == lat else lat - 1
    lat_to = lat if lt_max == lat else lat + 1

    lon_from = lon if ln_min == lon else lon - 1
    lon_to = lon if ln_max == lon else lon + 1

    time_from = time if t_min == time else time - 1
    time_to = time if t_max == time else time + 1

    for x in xrange(lat_from, lat_to + 1):
        for y in xrange(lon_from, lon_to + 1):
            for z in xrange(time_from, time_to + 1):
                if not (lat == x and lon == y and time == z):
                    result_tuples.append((str(x) + "_" + str(y) + "_" + str(z), cell_xi))

    return result_tuples


point = "5_4_5"
print get_direct_neighbor_ids(point, 1, 5, 1, 5, 1, 5, 55)
print len(get_direct_neighbor_ids(point, 1, 5, 1, 5, 1, 5, 55))
