import networkx as nx
import Misc.file_constants as constants
import Misc.utils as utils
import cPickle
import pandas as pd
import os
import csv



def create_graph(partitioned_dir, store_path, store=True, sim=False):
	all_days = os.listdir(partitioned_dir)
	print 'creating initial graph object'
	friend_graph = nx.Graph()
	for day_file in all_days:
		day_path = os.path.join(partitioned_dir, day_file)
		print 'reading datafram for current file:', day_path
		df = pd.read_csv(day_path)
		print 'adding edges to graph'
		for source, dest in df[[constants.SOURCE, constants.DEST]].values:
			friend_graph.add_edge(source, dest)
	if store:
		print 'storing graph at:', store_path
		with open(store_path, 'wb') as outfile:
			cPickle.dump(friend_graph,outfile)

	return True


def create_graph_directed(cdr_filename, store_path, store=True):
	print 'creating initial graph object'
	friend_graph = nx.MultiDiGraph()
	print 'loading data from', cdr_filename	
	chunksize = 10**6
	for data_chunk in pd.read_csv(cdr_filename, chunksize=chunksize):
		print 'Starting new chunk of size:', chunksize
		for source, dest, comm_type in data_chunk[[constants.SOURCE, constants.DEST, constants.COMM_TYPE]].values:
			if 'O' in comm_type:
				friend_graph.add_edge(source, dest)
			elif 'T' in comm_type: 
				friend_graph.add_edge(dest, source)
	if store:
		print 'storing graph at:', store_path
		with open(store_path, 'wb') as outfile:
			cPickle.dump(friend_graph,outfile)

	return True


def clean_dir_graph(graph_path, dest_path, mode):
	# mode=0 removes edges between users who don't have mutual exchanges A->B AND B->A
	# mode=1 (Mutual exchanges) AND (Total shared edges >= 5)
	# mode=2 (Mutual exchanges) AND (Total shared edges >= 10)
	with open(graph_path, 'rb') as infile:
		difriend_graph = cPickle.load(infile)
	unique_edges = set(difriend_graph.edges())
	for user_1, user_2 in unique_edges:
		both_directions = difriend_graph.has_edge(user_2, user_1)
		edge_count = len(difriend_graph[user_1][user_2])
		if both_directions:
			edge_count += len(difriend_graph[user_2][user_1])
			if (mode == 1) and (edge_count < 5):
				edges_bunch = [(user_1, user_2)] * edge_count
				difriend_graph.remove_edges_from(edges_bunch)
			elif (mode == 2) and (edge_count < 10):
				edges_bunch = [(user_1, user_2)] * edge_count
				difriend_graph.remove_edges_from(edges_bunch)
		else:
			edges_bunch = [(user_1, user_2)] * edge_count
			difriend_graph.remove_edges_from(edges_bunch)
	# new_graph = difriend_graph.to_undirected()
	with open(dest_path, 'wb') as outfile:
		cPickle.dump(difriend_graph, outfile)

	return difriend_graph



def get_distribution_encounters(encoutners_csv,network_object_path,destination_path):
	# row = [caller,caller_enc,delta_days,delta_seconds,tower,next_tower,last_time]
	friend_graph = cPickle.load(open(network_object_path,'rb'))
	caller_index = 0
	enc_index = 1
	nodes_set = set(nx.nodes(friend_graph))
	print 'loaded in ', len(nodes_set), 'nodes'
	with open(encoutners_csv,'rb') as csvfile:
		with open(destination_path,'wb') as csvout:
			csv_iterator = csv.reader(csvfile,delimiter=';')
			distance_csv = csv.writer(csvout,delimiter=';')
			for row in csv_iterator:
				caller = row[caller_index]
				encounteree = row[enc_index]
				if (caller in nodes_set) and (encounteree in nodes_set):
					try:
						distance = nx.shortest_path_length(friend_graph, source=caller, target=encounteree)
						row.append(distance)
						distance_csv.writerow(row)
					except Exception as e:
						pass	
	return True


def get_graph_distance(user1, user2, friend_graph):
	# -1 means they are infinitely far apart (not connected)
	# -2 means that one of the users was pruned from the graph
	if (user1 in friend_graph) and (user2 in friend_graph):
		try:
			return nx.shortest_path_length(friend_graph, source=user1, target=user2)
		except Exception as e:
			return -1
	return -2


if __name__ == '__main__':
    main()