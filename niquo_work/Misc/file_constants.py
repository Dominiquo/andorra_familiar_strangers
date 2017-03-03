JULY_DATA_ALL = '../../../data_repository/datasets/telecom/cdr/201607-AndorraTelecom-CDR.csv'
JULY_DATA_FILTERED = '../niquo_data/source_data/filtered201607-AndorraTelecom-CDR.csv'
TOWERS_ID = '../../workspace/yleng/towers1.csv'
TOWERS_TYPE = '../../../niquo_data/20151120-towersContainerTrip.csv'
FILTERED_PARTITIONED = '../../niquo_data/filtered_data/partitioned_towers/'


SOURCE = 'DS_CDNUMORIGEN'
TIMESTAMP = 'DT_CDDATAINICI'
TOWER_COLUMN = 'ID_CELLA_INI'
COMM_TYPE = 'ID_CDTIPUSCOM'
CARRIER = 'ID_CDOPERADORORIGEN'
DEST = 'DS_CDNUMDESTI'


TOWER_NUMBER = 'tower_id'
DATE_STRING = 'date'
DAYTIME = 'timestamp'

IDS_SET = set(['9500', '1060', '1075', '10', '3200', '2570', '445', '70', '2150',
       '130', '340', '9451', '1030', '1560', '3100', '160', '1590', '1111',
       '1', '9461', '100', '110', '30', '1095', '471', '2520', '3521',
       '3581', '3140', '1110', '1171', '480', '2020', '1140', '1580',
       '3090', '4502', '370', '1630', '140', '120', '3672', '3190', '1680',
       '210', '230', '2190', '342', '3050', '1091', '3530', '3020', '333',
       '2081', '5', '1760', '3590', '1050', '1735', '3040', '1121', '2070',
       '250', '3231', '311', '2041', 'tower_id', '420', '312', '220',
       '341', '3510', '1225', '231', '1040', '1010', '3690', '1080',
       '2120', '2060', '2201', '190', '1770', '3110', '1700', '222',
       '2170', '1780', '2090'])

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



