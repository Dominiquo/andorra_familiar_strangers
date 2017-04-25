import networkx as nx
import Misc.file_constants as constants
import Misc.utils as utils
import cPickle
import pandas as pd
import os


def get_social_encounters(social_path, encounters_path, dest_path, save_file=True):
	NO_PATH = -1
	print 'loading social graph...'
	social_graph = utils.load_pickle(social_path)
	print 'loading encounters graph...'
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
