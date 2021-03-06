import networkx as nx
import Misc.file_constants as constants
import Misc.utils as utils
import cPickle
import pandas as pd
import os


NO_PATH = -1
TOWER_ENCS_DIR = 'tower_encounters'


def get_social_encounters(social_path, encounters_path, dest_path, save_file=True):
	print 'loading social graph:', social_path
	social_graph = utils.load_pickle(social_path)
	print 'loading encounters graph:', encounters_path
	encs_graph = utils.load_pickle(encounters_path)
	encs_edges_set = set(encs_graph.edges())
	df_dict = {constants.USER_1: [], constants.USER_2: [], constants.ENCS_COUNT: [], constants.SOC_DIST: []}
	print 'iterating through', len(encs_edges_set), 'edges.'
	disconnected_edges = 0
	count = 0
	hund = len(encs_edges_set)/100
	for source, dest in encs_edges_set:
		count += 1
		if count % hund == 0:
			print float(count)/(hund*100)
		encs_count = len(encs_graph[source][dest])
		df_dict[constants.USER_1].append(source)
		df_dict[constants.USER_2].append(dest)
		df_dict[constants.ENCS_COUNT].append(encs_count)
		try:
			distance = nx.shortest_path_length(social_graph, source, dest)
			df_dict[constants.SOC_DIST].append(distance)
		except Exception as e:
			disconnected_edges += 1
			df_dict[constants.SOC_DIST].append(NO_PATH)
			pass
	print 'edges without a social connection:', disconnected_edges
	dataframe = pd.DataFrame(df_dict)
	if save_file:
		dataframe.to_csv(dest_path, index=False)
	return dataframe


def get_social_encounters(social_path_0, social_path_1, social_path_2, encounters_path, dest_path, save_file=True):
	print 'loading social graph:', social_path_0
	social_graph_0 = utils.load_pickle(social_path_0)
	print 'loading social graph:', social_path_1
	social_graph_1 = utils.load_pickle(social_path_1)
	print 'loading social graph:', social_path_2
	social_graph_2 = utils.load_pickle(social_path_2)

	print 'loading encounters graph:', encounters_path
	encs_graph = utils.load_pickle(encounters_path)
	encs_edges_set = set(encs_graph.edges())
	df_dict = {constants.USER_1: [], constants.USER_2: [], constants.ENCS_COUNT: [], constants.MODE_0_DIST: [],
				constants.MODE_1_DIST: [], constants.MODE_2_DIST: []}
	print 'iterating through', len(encs_edges_set), 'edges.'
	disconnected_edges = 0
	count = 0
	hund = len(encs_edges_set)/100
	for source, dest in encs_edges_set:
		count += 1
		if count % hund == 0:
			print float(count)/(hund*100)
		encs_count = len(encs_graph[source][dest])
		df_dict[constants.USER_1].append(source)
		df_dict[constants.USER_2].append(dest)
		df_dict[constants.ENCS_COUNT].append(encs_count)

		if (source in social_graph_0) and (dest in social_graph_0) and (nx.has_path(social_graph_0, source, dest)):
			distance_0 = nx.shortest_path_length(social_graph_0, source, dest)
			df_dict[constants.MODE_0_DIST].append(distance_0)
		else:
			df_dict[constants.MODE_0_DIST].append(NO_PATH)

		if (source in social_graph_1) and (dest in social_graph_1) and (nx.has_path(social_graph_1, source, dest)):
			distance_1 = nx.shortest_path_length(social_graph_1, source, dest)
			df_dict[constants.MODE_1_DIST].append(distance_1)
		else:
			df_dict[constants.MODE_1_DIST].append(NO_PATH)

		if (source in social_graph_2) and (dest in social_graph_2) and (nx.has_path(social_graph_2, source, dest)):
			distance_2 = nx.shortest_path_length(social_graph_2, source, dest)
			df_dict[constants.MODE_2_DIST].append(distance_2)
		else:
			df_dict[constants.MODE_2_DIST].append(NO_PATH)

	print 'edges without a social connection:', disconnected_edges
	dataframe = pd.DataFrame(df_dict)
	if save_file:
		dataframe.to_csv(dest_path, index=False)
	return dataframe


def get_encounters_for_pairs(root_path, dest_path):
	encs_dict = {}
	root = root_path.split('/')[-1]
	encs_path = os.path.join(root_path, TOWER_ENCS_DIR)
	print '**************************'
	print 'entering root path:', root
	for day_dir in os.listdir(encs_path):
		day_path = os.path.join(encs_path, day_dir)
		print '________________'
		print 'opening day path:', day_path
		for tower_file in os.listdir(day_path):
			print 'opening tower file:', tower_file
			tower_path = os.path.join(day_path, tower_file)
			tower_graph = utils.load_pickle(tower_path)
			tower_edges = set(tower_graph.edges())
			for user_1, user_2 in tower_edges:
				count = len(tower_graph[user_1][user_2])
				add_encs_count(encs_dict, user_1, user_2, count)
			del tower_graph
	with open(dest_path, 'wb') as outfile:
		cPickle.dump(encs_dict, outfile)
	return encs_dict


def add_encs_count(encs_dict, user_1, user_2, count):
	source = max(user_1, user_2)
	dest = min(user_1, user_2)
	key = (source, dest)
	if (key in encs_dict):
		encs_dict[key] += count
	else:
		encs_dict[key] = count


def get_2016_months_encs():
	# root_paths = ['/home/niquo/niquo_data/201507-AndorraTelecom-CDR',
	# '/home/niquo/niquo_data/201508-AndorraTelecom-CDR',
	# '/home/niquo/niquo_data/201509-AndorraTelecom-CDR',
	# '/home/niquo/niquo_data/201510-AndorraTelecom-CDR',
	# '/home/niquo/niquo_data/201511-AndorraTelecom-CDR',
	# '/home/niquo/niquo_data/201512-AndorraTelecom-CDR']

	root_paths = ['/home/niquo/niquo_data/201601-AndorraTelecom-CDR',
	'/home/niquo/niquo_data/201602-AndorraTelecom-CDR',
	'/home/niquo/niquo_data/201603-AndorraTelecom-CDR',
	'/home/niquo/niquo_data/201604-AndorraTelecom-CDR',
	'/home/niquo/niquo_data/201605-AndorraTelecom-CDR',
	'/home/niquo/niquo_data/201606-AndorraTelecom-CDR',
	'/home/niquo/niquo_data/201607-AndorraTelecom-CDR',
	'/home/niquo/niquo_data/201608-AndorraTelecom-CDR',
	'/home/niquo/niquo_data/201609-AndorraTelecom-CDR']

	dest_root_dir = '/home/niquo/niquo_data/spring_results_data/'
	for root in root_paths:
		dest_file = 'encs_count_dict.p'
		dest_dir = os.path.join(dest_root_dir, root)
		dest_path = os.path.join(dest_dir, dest_file)
		get_encounters_for_pairs(root, dest_path)


