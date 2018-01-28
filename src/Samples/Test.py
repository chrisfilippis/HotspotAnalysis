import pandas
import gmplot

def get_decimal(_text):
    return int(_text) * 0.01


def create_heatmap(path, html_path):
    fields = ['point', 'gi']

    data = pandas.read_csv(path, sep=',', header=None, names=fields)

    latitudes = []
    longitudes = []

    for index, row in data.iterrows():
        latitudes.append(get_decimal(row['point'].split('_')[1]))
        longitudes.append(get_decimal(row['point'].split('_')[0]))

    print latitudes
    print longitudes

    gmap = gmplot.GoogleMapPlotter(latitudes[0], longitudes[1], 5)

    # gmap.plot(latitudes, longitudes, 'cornflowerblue', edge_width=10)
    gmap.scatter(latitudes, longitudes, '#3B0B39', size=40, marker=False)
    # gmap.scatter(latitudes, longitudes, 'k', marker=True)
    # gmap.heatmap(latitudes, longitudes)
    gmap.draw(html_path)


def create_testheatmap(path, html_path):
    fields = ['t', 'tt', 'lon', 'lat']

    data = pandas.read_csv('C:\Spark_Data\\10K_bigdata.sample', sep=' ', header=None, names=fields)

    latitudes = []
    longitudes = []

    for index, row in data.iterrows():
        latitudes.append(row['lat'])
        longitudes.append(row['lon'])

    print data

    gmap = gmplot.GoogleMapPlotter(latitudes[0], longitudes[1], 5)

    gmap.plot(latitudes, longitudes, 'cornflowerblue', edge_width=10)
    # gmap.scatter(latitudes, longitudes, '#3B0B39', size=40, marker=False)
    # gmap.scatter(latitudes, longitudes, 'k', marker=True)
    gmap.heatmap(latitudes, longitudes)
    gmap.draw(html_path)


# create_testheatmap('C:\Spark_Data\output\data_out.csv', 'mymap.html')
create_heatmap('C:\Spark_Data\output\data_out.csv', 'mymap.html')
