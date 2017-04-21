import pandas as pd
import Main


USE_MONTHS = ['201507-AndorraTelecom-CDR.csv',
 '201508-AndorraTelecom-CDR.csv',
 '201509-AndorraTelecom-CDR.csv',
 '201510-AndorraTelecom-CDR.csv',
 '201511-AndorraTelecom-CDR.csv',
 '201512-AndorraTelecom-CDR.csv']

 DATA_DIR = '/home/niquo/niquo_data'


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
	return True


# partitioned_data_path = Main.partition_data(root_path, all_data_path)
# condense_data_path = Main.condense_data(root_path, partitioned_data_path)
# encs_path = Main.find_encounters(root_path, condense_data_path)