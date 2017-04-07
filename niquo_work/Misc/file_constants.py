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

DAY = 'DATE'
MAX_TIME = 'timestamp amax'
MIN_TIME = 'timestamp amin'
MATCHING = 'same'


TOWER_NUMBER = 'tower_id'
DATE_STRING = 'date'
DAYTIME = 'timestamp'
TIME_BLOCK = 'time_chunk'

USEFUL_ROWS = [SOURCE, TIMESTAMP, TOWER_COLUMN, DEST]



'''	1 = shoppping
	2 = nature interests
	3 = wellness interest
	4 = leisure interests
	5 = gastronomic interests
	6 = culture interests
	7 = events interest
	8 = None
	9 = other '''



