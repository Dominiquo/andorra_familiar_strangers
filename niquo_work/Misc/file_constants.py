JULY_DATA_ALL = '/home/data_repository//datasets/telecom/cdr/201607-AndorraTelecom-CDR.csv'
JULY_DATA_FILTERED = '/home/niquo/niquo_data/source_data/filtered201607-AndorraTelecom-CDR.csv'
TOWERS_ID = '/home/workspace/yleng/towers1.csv'
TOWERS_TYPE = '/home/niquo/niquo_data/20151120-towersContainerTrip.csv'
FILTERED_PARTITIONED = '/home/niquo/niquo_data/filtered_data/partitioned_towers/'
COMM_DATA  = 'S-CDR'
FIRST_CALL = '/home/workspace/yleng/first_call_2016_largerThan10.csv'
FILTERED_MONTHS = '/home/workspace/yleng/filtered'



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

USEFUL_ROWS = [SOURCE, TIMESTAMP, TOWER_COLUMN, DEST, CARRIER]


USER_1 = 'USER_1'
USER_2 = 'USER_2'
ENCS_COUNT = 'ENCS_COUNT'
SOC_DIST = 'SOC_DIST'


# ENCOUNTER AND FRIEND GRAPH FILE NAMES
PAIRS_CSV = 'new_friend_encs.csv'
ENCS_DICT = 'encs_count_dict.p'
BASE_DIGRAPH = 'social_digraph.p'
MODE_0_GRAPH = 'filtered_graph_mode_0.p'
MODE_0_DIST = 'mode_0_dist'
MODE_1_GRAPH = 'filtered_graph_mode_1.p'
MODE_1_DIST = 'mode_1_dist'
MODE_2_GRAPH = 'filtered_graph_mode_2.p'
MODE_2_DIST = 'mode_2_dist'


# FIRST CALL COLUMN CONSTANTS
CALL_LEN = 'call_length'
CALLS = 'calls'
FC_DAY = 'day'
FC_FIRST = 'first'
TEXTS = 'texts'
FIRST_CALL_COLS = [CALL_LEN, CALLS, FC_DAY, FC_FIRST, TEXTS]


'''	1 = shoppping
	2 = nature interests
	3 = wellness interest
	4 = leisure interests
	5 = gastronomic interests
	6 = culture interests
	7 = events interest
	8 = None
	9 = other '''



