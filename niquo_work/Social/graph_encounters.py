import networkx as nx
import Misc.file_constants as constants
import Misc.utils as utils
import cPickle
import pandas as pd
import os


def get_social_encounters(social_path, encounters_path, dest_path):
	social_graph = utils.load_pickle(social_path)
	encs_graph = utils.load_pickle(encounters_path)
	for source, dest in encs_graph.edges():
		encs_count = len(encs_graph[source][dest])
		try:
			distance = nx.shortest_path(social_graph, source, dest)
		except Exception as e:
			pass

		# TODO: something here

	return None
