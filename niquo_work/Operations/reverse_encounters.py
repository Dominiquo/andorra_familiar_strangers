import pandas as pd
import Misc.file_constants as constants
import Misc.utils as utils
import Main


USE_MONTHS = ['201507-AndorraTelecom-CDR.csv',
 '201508-AndorraTelecom-CDR.csv',
 '201509-AndorraTelecom-CDR.csv',
 '201510-AndorraTelecom-CDR.csv',
 '201511-AndorraTelecom-CDR.csv',
 '201512-AndorraTelecom-CDR.csv']


def get_new_friend_set(new_friend_csv):
	print 'reading new friend csv..'
	friend_df = pd.read_csv(new_friend_csv)
	first_col = friend_df.columns[0]
	user_list = []
	print 'iterating through', len(friend_df) , 'values and splitting their pairs'
	for val in friend_df[first_col].values:
		s,t = val.split('_')
		user_list.append(s)
		user_list.append(t)
	return set(user_list)


def create_maps_prev_six_months():
	DATA_DIR = '/home/niquo/niquo_data'
	print 'retreiving friend set from', constants.FIRST_CALL
	friend_set = get_new_friend_set(constants.FIRST_CALL)
	month_filter = lambda row: row[constants.SOURCE] in friend_set
	chunk_size = 10
	print 'iterating through months to start encounter process'
	for month in USE_MONTHS:
		dir_str = month.split('.')[0]
		root_path = utils.create_dir(DATA_DIR, dir_str)
		print 'current month root path:', root_path
		csv_month = os.path.join(constants.FILTERED_MONTHS, month)
		print 'current data path:', csv_month
		print 'partitioning data...'
		partitioned_data_path = Main.partition_data(root_path, csv_month, filter_func=month_filter)
		print 'condensing data...'
		condense_data_path = Main.condense_data(root_path, partitioned_data_path, chunk_size=10)
		print 'finding encoutners...'
		encs_path = Main.find_encounters(root_path, condense_data_path, enc_window=chunk_size)

	return True
