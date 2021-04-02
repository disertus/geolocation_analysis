
###########################################
#%%

import bz2
import csv
import geopandas as gp
import os
import pandas as pd
from tqdm import tqdm


project_path = os.getcwd()
archive_name_html = 'HTML_NAME'
archive_name_mobile = 'MOBILE_NAME'

html_archive_path = f'{project_path}/data/source_data/{archive_name_html}'
mobile_archive_path = f'{project_path}/data/source_data/{archive_name_mobile}'


def filter_out_low_speed_test_data(reading_file_path: str, writing_file_path: str, min_speed: int, max_latency: int):
    with open(f'{project_path}/data/resulting_data/{reading_file_path}', 'r') as filtered_file:
        with open(f'{project_path}/data/resulting_data/{writing_file_path}', 'w') as writefile:
            csv_reader = csv.reader(filtered_file)
            csv_writer = csv.writer(writefile)
            csv_writer.writerow(next(csv_reader))
            for row in tqdm(csv_reader):
                try:
                    if int(row[8]) < min_speed:
                        continue
                    elif int(row[10]) > max_latency:
                        continue
                    else:
                        csv_writer.writerow(row)
                        writefile.flush()
                except ValueError:
                    continue


def filter_out_unnecessary_fields(list_of_data):
    data = []
    for e in [0, 58, 55, 59, 60, 61, 62, 71, 74, 76, 78, 79, 57, 28, 27, 24, 23, 22, 21, 11, 12, 10, 8, 9]:
        data.append(list_of_data[e])
    return data


def write_filtered_data(file_path: str, resuling_csv_name: str):
    with open(f'{project_path}/data/resulting_data/{resuling_csv_name}', 'w') as writer:
        csv_writer = csv.writer(writer)
        for item in tqdm(bz2.open(file_path, mode='rb')):
            csv_writer.writerow(filter_out_unnecessary_fields(item.decode('utf-8').split('\t')))
            writer.flush()


write_filtered_data(html_archive_path, 'filtered_html.csv')
write_filtered_data(mobile_archive_path, 'filtered_mobile.csv')

filter_out_low_speed_data('filtered_html.csv', 'filtered_speed_and_latency_html.csv', min_speed=30000, max_latency=20)
filter_out_low_speed_data('filtered_mobile.csv', 'filtered_mobile.csv', min_speed=30000, max_latency=20)


#####################################################
# end of initial filtering part


#%%
# open the file with coordinates, filter out the records without the new ID

settlements_polygons_dataframe = gp.read_file('COORD_PATH')
settlements_polygons_dataframe = settlements_polygons_dataframe[~(settlements_polygons_dataframe['Chk'] == "")]
settlements_polygons_dataframe.info()

#%%
# open the speedtests resutls, filter out mobile

def open_filtered_speedtests_csv_file(file_name: str):
    return pd.read_csv(f'{os.getcwd()}/data/resulting_data/{file_name}')


def filter_out_mobile_isps(dataframe):
    return dataframe[~dataframe['ISP'].str.contains('kyivstar|lifecell|vodafone|intertelecom|vf ukraine',
                                                    case=False, na=False)]

# insert required filenames here
dataframe_speedtests_html = open_filtered_csv_file('filtered_speed_and_latency_html.csv')
dataframe_speedtests_mobile = open_filtered_csv_file('filtered_speed_and_latency_mobile.csv')
dataframe_speedtests_html = filter_out_mobile_isps(dataframe_speedtests_html)
dataframe_speedtests_mobile = filter_out_mobile_isps(dataframe_speedtests_mobile)

# concatenate html and mobile speedtests

dataframe_speedtests = pd.concat([dataframe_speedtests_html, dataframe_speedtests_mobile])
dataframe_speedtests.info()

#%%

def filter_tests_by_speed_and_latency(dataframe, speed_kbps: int, latency_ms: int):
    return dataframe.query(f'DOWNLOAD >= {speed_kbps} & LATENCY <= {latency_ms}')


def filter_low_accuracy_tests(dataframe, location_accuracy_max: int):
    return dataframe.query(f'ACCURACY >= {location_accuracy_max}')


tests_50mbps = filter_tests_by_speed_and_latency(dataframe_speedtests, speed_kbps=50000, latency_ms=20)
tests_30mbps = filter_tests_by_speed_and_latency(dataframe_speedtests, speed_kbps=30000, latency_ms=10)

# comment out if not interested in location accuracy
tests_50mbps = filter_low_accuracy_tests(tests_50mbps, location_accuracy_max=10000)
tests_30mbps = filter_low_accuracy_tests(tests_30mbps, location_accuracy_max=10000)

tests_50mbps.info()
tests_30mbps.info()

#%%
# create dataframes with Point objects containing x/y coordinate paris


def create_points_from_coordinate_pairs(dataframe, lng_column_name:str, lat_column_name:str):
    return gp.GeoDataFrame(dataframe, geometry=gp.points_from_xy(dataframe[lng_column_name], dataframe[lat_column_name]))


speedtests_30mbps_gp_dataframe = create_points_from_coordinate_pairs(tests_30mbps,
                                                                     lng_column_name='LNG',
                                                                     lat_column_name='LAT')
speedtests_50mbps_gp_dataframe = create_points_from_coordinate_pairs(tests_50mbps,
                                                                     lng_column_name='LNG',
                                                                     lat_column_name='LAT')
speedtests_30mbps_gp_dataframe.info()
speedtests_50mbps_gp_dataframe.info()

#%%
# perform a spatial join: check if any of settlements' polygons contain coordinate points from speedtests

def check_if_polygon_contains_coord_point(df_with_polygons, df_with_points):
    return gp.sjoin(df_with_polygons, df_with_points, how='left', op='contains')

point_in_polygon_30mbps = check_if_polygon_contains_coord_point(settlements_polygons_dataframe, speedtests_30mbps_gp_dataframe)
point_in_polygon_50mbps = check_if_polygon_contains_coord_point(settlements_polygons_dataframe, speedtests_50mbps_gp_dataframe)

point_in_polygon_30mbps.info()
point_in_polygon_50mbps.info()

#%%

def prepare_df_for_export(dataframe):
    dataframe = dataframe[['SOME_FIELDS']]
    dataframe = dataframe[['SOME_OTHER_FIELDS']].groupby(['Chk']).count()
    dataframe = dataframe.reset_index()
    return dataframe


df_30mbps_for_export = prepare_df_for_export(point_in_polygon_30mbps)
df_50mbps_for_export = prepare_df_for_export(point_in_polygon_50mbps)

df_for_export = pd.merge(df_30mbps_for_export, df_50mbps_for_export, how='left', on='Chk', suffixes=('_30mbps/10ms', '_50mbps/20ms'))
df_for_export.rename(columns={'Chk': 'KOATUU_new'}, inplace=True)

df_for_export.info()
print(df_for_export.head(10))

df_for_export.to_csv('latest__loc_accuracy.csv')

