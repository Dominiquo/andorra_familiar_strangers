import pandas as pd
import Misc.file_constants as constants
import Misc.utils as utils
import networkx as nx
import Social.Network as net
import Structures.InteractionMap as imap
import Main
import os


USE_MONTHS = ['201507-AndorraTelecom-CDR',
	'201508-AndorraTelecom-CDR',
	'201509-AndorraTelecom-CDR',
	'201510-AndorraTelecom-CDR',
	'201511-AndorraTelecom-CDR',
	'201512-AndorraTelecom-CDR',
	'201601-AndorraTelecom-CDR',
	'201602-AndorraTelecom-CDR',
	'201603-AndorraTelecom-CDR',
	'201604-AndorraTelecom-CDR',
	'201605-AndorraTelecom-CDR',
	'201606-AndorraTelecom-CDR',
	'201607-AndorraTelecom-CDR',
	'201608-AndorraTelecom-CDR',
	'201609-AndorraTelecom-CDR']

def get_new_friend_set(new_friend_csv, user_pair_set=None):
	if user_pair_set == None:
		user_pair_set = get_new_friend_pair_set(new_friend_csv)
	all_users = []
	for u1,u2 in user_pair_set:
		all_users.append(u1)
		all_users.append(u2)

	return set(all_users)

def get_new_friend_pair_set(new_friend_csv):
	full_df = split_users_first_call_csv(new_friend_csv)
	return set([(u1,u2) for u1,u2 in full_df[[constants.USER_1, constants.USER_2]].values])	

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
	encs_dict = utils.load_pickle(encs_path)
	friend_df = split_users_first_call_csv(first_call_csv)
	friends_set = set([(user1, user2) for user1,user2 in friend_df[[constants.USER_1, constants.USER_2]].values])
	intersection_pairs = friends_set.intersection(set(encs_dict.keys()))
	relevant_encs = {k:encs_dict[k] for k in intersection_pairs}

	mode_0_path = os.path.join(root_path, constants.MODE_0_GRAPH)
	print 'loading graph mode 0:', mode_0_path
	mode_0_graph = utils.load_pickle(mode_0_path).to_undirected()

	mode_1_path = os.path.join(root_path, constants.MODE_1_GRAPH)
	print 'loading graph mode 1:', mode_1_path
	mode_1_graph = utils.load_pickle(mode_1_path).to_undirected()

	mode_2_path = os.path.join(root_path, constants.MODE_2_GRAPH)
	print 'loading graph mode 2:', mode_2_path
	mode_2_graph = utils.load_pickle(mode_2_path).to_undirected()

	friend_df[constants.ENCS_COUNT] = friend_df.apply(lambda row: apply_encs(relevant_encs, row), axis=1)
	friend_df[constants.MODE_0_DIST] = friend_df.apply(lambda row: apply_distance(mode_0_graph, row), axis=1)
	friend_df[constants.MODE_1_DIST] = friend_df.apply(lambda row: apply_distance(mode_1_graph, row), axis=1)
	friend_df[constants.MODE_2_DIST] = friend_df.apply(lambda row: apply_distance(mode_2_graph, row), axis=1)

	friend_df = friend_df[friend_df[constants.ENCS_COUNT] >= 0]
	dest_path = os.path.join(root_path, dest_filename)
	print 'storing dataframe at ', dest_path
	friend_df.to_csv(dest_path, index=False)
	return friend_df

def combine_dataframes(df_paths, months, how='outer'):
	combine_cols = constants.FIRST_CALL_COLS + [constants.USER_1, constants.USER_2]
	dfs_list = []
	month_df = zip(months, df_paths)

	for month, df_p in month_df:
		print 'getting df from', df_p
		df = pd.read_csv(df_p)
		rename_dict = {constants.MODE_0_DIST: 'soc0_' + str(month), constants.MODE_1_DIST: 'soc1_' + str(month),
						constants.MODE_2_DIST: 'soc2_' + str(month), constants.ENCS_COUNT: 'encs_' + str(month)}
		df = df.rename(columns=rename_dict)
		dfs_list.append(df)
	combined_df = reduce(lambda x, y: pd.merge(x, y, how=how, on=combine_cols), dfs_list)
	return combined_df

def apply_encs(encs_dict, row):
	NO_ENCS = -1
	source = row[constants.USER_1]
	dest = row[constants.USER_2]
	key = (source, dest)
	if key in encs_dict:
		return encs_dict[key]
	else:
		return NO_ENCS

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


def create_encs_df_all():
	root_dirs = ['201507-AndorraTelecom-CDR',
	'201508-AndorraTelecom-CDR',
	'201509-AndorraTelecom-CDR',
	'201510-AndorraTelecom-CDR',
	'201511-AndorraTelecom-CDR',
	'201512-AndorraTelecom-CDR',
	'201601-AndorraTelecom-CDR',
	'201602-AndorraTelecom-CDR',
	'201603-AndorraTelecom-CDR',
	'201604-AndorraTelecom-CDR',
	'201605-AndorraTelecom-CDR',
	'201606-AndorraTelecom-CDR',
	'201607-AndorraTelecom-CDR',
	'201608-AndorraTelecom-CDR',
	'201609-AndorraTelecom-CDR']

	data_root = '/home/niquo/niquo_data'
	for directory in root_dirs:
		root_path = os.path.join(data_root, directory)
		create_encs_df_select_friends(constants.FIRST_CALL, root_path)


def combine_maps_for_months(data_dir='/home/niquo/niquo_data',months_paths=USE_MONTHS):
	print 'combing interactin graphs for data from:', data_dir
	TOWER_ENCS = 'tower_encounters'
	for dir_str in months_paths:
		month_path = os.path.join(data_dir, dir_str)
		print 'combing for month path:', month_path
		inter_map_obj = imap.InteractionMap(month_path)
		tower_data_path = os.path.join(month_path, TOWER_ENCS)
		inter_map_obj.combine_all_graphs(tower_data_path)
	print 'complete.'
	return True

def create_maps_for_months(data_dir='/home/niquo/niquo_data',months_paths=USE_MONTHS, new_friend_csv=constants.FIRST_CALL):
	print 'retreiving friend set from', new_friend_csv
	pair_set = get_new_friend_pair_set(new_friend_csv)
	friend_set = get_new_friend_set(new_friend_csv, pair_set)
	month_filter = lambda row: row[constants.SOURCE] in friend_set
	chunk_size = 10
	print 'iterating through months to start encounter process'
	for dir_str in months_paths:
		month = dir_str + '.csv'
		root_path = utils.create_dir(data_dir, dir_str)
		print 'current month root path:', root_path
		csv_month = os.path.join(constants.FILTERED_MONTHS, month)
		print 'current data path:', csv_month
		print 'partitioning data...'
		partitioned_data_path = Main.partition_data(root_path, csv_month, filter_func=month_filter)
		print 'condensing data...'
		condense_data_path = Main.condense_data(root_path, partitioned_data_path, chunk_size=10)
		print 'finding encoutners...'
		encs_path = Main.find_encounters(root_path, condense_data_path, enc_window=chunk_size, user_pair_set=pair_set)
		# digraph_base_store_path = os.path.join(root_path, constants.BASE_DIGRAPH)
		# print 'creating base directed graph to be stored at', digraph_base_store_path
		# net.create_graph_directed(csv_month, digraph_base_store_path)
		# for mode in range(3):
		# 	filtered_graph_name = 'filtered_graph_mode_' + str(mode) + '.p'
		# 	filt_graph_store_path = os.path.join(root_path, filtered_graph_name)
		# 	print 'creating graph for mode', mode, 'to be stored at ', filtered_graph_name
		# 	net.clean_dir_graph(digraph_base_store_path, filt_graph_store_path, mode)

	return True
