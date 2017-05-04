import pandas as pd
import Misc.file_constants as constants
import Misc.utils as utils
import networkx as nx
import Main
import os


USE_MONTHS = ['201601-AndorraTelecom-CDR.csv',
 '201602-AndorraTelecom-CDR.csv',
 '201603-AndorraTelecom-CDR.csv',
 '201604-AndorraTelecom-CDR.csv',
 '201605-AndorraTelecom-CDR.csv',
 '201606-AndorraTelecom-CDR.csv',
 '201607-AndorraTelecom-CDR.csv',
 '201608-AndorraTelecom-CDR.csv',
 '201609-AndorraTelecom-CDR.csv',
 '201610-AndorraTelecom-CDR.csv']

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

def split_users_first_call_csv(new_friend_csv):
	print 'reading new friend csv..'
	friend_df = pd.read_csv(new_friend_csv)
	first_col = friend_df.columns[0]
	user_list = []
	get_source = lambda combo_str: max(combo_str.split('_'))
	get_dest = lambda combo_str: min(combo_str.split('_'))
	print 'making columns of source users in contact'
	friend_df[constants.USER_1] = friend_df[first_col].apply(get_source)
	print 'making column of dest users in contact'
	friend_df[constants.USER_2] = friend_df[first_col].apply(get_dest)
	del friend_df[first_col]
	return friend_df


def create_encs_df_select_friends(first_call_csv, root_path, dest_filename=constants.PAIRS_CSV):
	encs_path = os.path.join(root_path, constants.ENCS_DICT)
	print 'loading all encounter pairs from:', encs_path
	with open(encs_path, 'rb') as infile:
		encs_dict = cPickle.load(infile)
	friend_df = split_users_first_call_csv(first_call_csv)
	friends_set = set([(user1, user2) for user1,user2 in friend_df[[constants.USER_1, constants.USER_2]].values])
	intersection_pairs = friend_set.intersection(set(encs_dict.keys))
	relevant_encs = {k:encs_dict[k] for k in intersection_pairs}
	print 'deleting encs_dict...'
	del encs_dict

	mode_0_path = os.path.join(root_path, constants.MODE_0_GRAPH)
	print 'loading graph mode 0:', mode_0_path
	with open(mode_0_path) as infile:		
		mode_0_graph = cPickle.load(infile).to_undirected()

	mode_1_path = os.path.join(root_path, constants.MODE_1_GRAPH)
	print 'loading graph mode 1:', mode_1_path
	with open(mode_1_path) as infile:
		mode_1_graph = cPickle.load(infile).to_undirected()

	mode_2_path = os.path.join(root_path, constants.MODE_2_PATH)
	print 'loading graph mode 2:', mode_2_path
	with open(mode_2_path) as infile:
		mode_2_graph = cPickle.load(infile).to_undirected()

	friend_df[constants.ENCS_COUNT] = df.apply(lambda row: apply_encs(relevant_encs, row), axis=1)
	friend_df[constants.MODE_0_DIST] = df.apply(lambda row: apply_distance(mode_0_graph, row), axis=1)
	friend_df[constants.MODE_1_DIST] = df.apply(lambda row: apply_distance(mode_1_graph, row), axis=1)
	friend_df[constants.MODE_2_DIST] = df.apply(lambda row: apply_distance(mode_2_graph, row), axis=1)

	friend_df = friend_df[friend_df[constants.ENCS_COUNT] >= 0]
	dest_path = os.path.join(root_path, dest_filename)
	print 'storing dataframe at ', dest_path
	friend_df.to_csv(dest_path)
	return friend_df

def apply_encs(encs_dict, row):
	source = row[constants.USER_1]
	dest = row[constants.USER_2]
	key = (source, dest)
	if key in encs_dict:
		return encs_dict[key]
	else:
		return -1

def apply_distance(soc_graph, row):
	NO_PATH = -1
	source = row[constants.USER_1]
	dest = row[constants.USER_2]
	try:
		distance = nx.shortest_path_length(soc_graph, source, dest)
	except Exception as e:
		distance = NO_PATH
		pass
	return distance

def create_maps_for_months():
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
