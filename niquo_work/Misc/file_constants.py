JULY_DATA_ALL = '../../../data_repository/datasets/telecom/cdr/201607-AndorraTelecom-CDR.csv'
JULY_DATA_FILTERED = '../niquo_data/source_data/filtered201607-AndorraTelecom-CDR.csv'
TOWERS_ID = '../../workspace/yleng/towers1.csv'
TOWERS_TYPE = '../../../niquo_data/20151120-towersContainerTrip.csv'
FILTERED_PARTITIONED = '../../niquo_data/filtered_data/partitioned_towers/'
COMM_DATA  = 'S-CDR'



SOURCE = 'DS_CDNUMORIGEN'
TIMESTAMP = 'DT_CDDATAINICI'
TOWER_COLUMN = 'ID_CELLA_INI'
COMM_TYPE = 'ID_CDTIPUSCOM'
CARRIER = 'ID_CDOPERADORORIGEN'
DEST = 'DS_CDNUMDESTI'


TOWER_NUMBER = 'tower_id'
DATE_STRING = 'date'
DAYTIME = 'timestamp'
HOUR = 'hour'

IDS_SET = set([ 471,    1, 1075,  110,  420,   30, 3200, 1630, 9500,  160, 1735,
       1590,  120, 3581, 1030, 1010,   10,  100, 1140, 1560, 2081, 9461,
       3050, 9451, 3521,   70, 2570, 3510, 1111,  231, 1680, 1760, 1171,
       2150, 1110,  445,    5,  220,  370,  480, 2041,  250, 1095, 2190,
        340, 2520, 3020,  130, 1060,  342, 1770, 3672, 4502,  140, 2020,
       3040, 2070, 3530, 1225, 1050, 1040,  341, 2170, 3100, 3231, 1091,
       3190, 1580, 1700,  210,  312,  190, 3590, 3110, 1080,  333, 2201,
       2120,  230, 3090,  311, 3140, 1121, 3690,  222, 1780, 2060, 2090])

USEFUL_ROWS = [SOURCE, TIMESTAMP, TOWER_COLUMN, COMM_TYPE, CARRIER, DEST]



'''	1 = shoppping
	2 = nature interests
	3 = wellness interest
	4 = leisure interests
	5 = gastronomic interests
	6 = culture interests
	7 = events interest
	8 = None
	9 = other '''



