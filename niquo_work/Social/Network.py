import networkx as nx
import Misc.file_constants as constants
import cPickle
import os
import csv

def create_graph(partitioned_dir, store_path, store=True, sim=False):
	all_days = os.path.listdir(partitioned_dir)
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

def clean_graph(network_object_path, cleaned_path, threshold=1000):
	friend_graph = cPickle.load(open(network_object_path,'rb'))
	nodes = nx.nodes(friend_graph)
	degrees_dict = nx.degree(friend_graph,nodes)
	count = 0
	for node, degree in degrees_dict.iteritems():
		if degree > threshold:
			friend_graph.remove_node(node)
			count += 1
	print 'removed ', count, 'nodes from the graph'
	cPickle.dump(friend_graph,open(cleaned_path,'wb'))
	return True


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

def make_new_csv_without_B(csv_file,user_hash_dict,destination_path):
	csvData = raw.RawCDRCSV(csv_file)
	caller_index = 0
	comm_type_index = 10
	receiver_index = 16
	nodes_converted = 0
	with open(destination_path,'wb') as outfile:
		csvout = csv.writer(outfile,delimiter=';')
		for row in csvData.rows_generator():
			caller = row[caller_index]
			receiver = row[receiver_index]
			comm_type = row[comm_type_index]
			if caller in user_hash_dict:
				nodes_converted += 1
				caller = user_hash_dict[caller]
			if receiver in user_hash_dict:
				nodes_converted += 1
				receiver = user_hash_dict[receiver]
			csvout.writerow([caller,receiver,comm_type])

	print 'total number of nodes mapped to another node:', nodes_converted
	return True

def make_new_friendship_graph(doubled_CSV, destination_path, limit=100):
	new_graph_obj = friend_graph = nx.Graph()
	csvData = raw.RawCDRCSV(doubled_CSV)
	caller_index = 0
	receiver_index = 1
	comm_type_index = 2
	skipped_rows = 0
	for row in csvData.rows_generator():
		caller = row[caller_index]
		receiver = row[receiver_index]
		comm_type = row[comm_type_index]
		if caller != receiver:
			new_graph_obj.add_edge(caller,receiver)
		else:
			skipped_rows += 1
	print 'total number of rows skipped:', skipped_rows
	print 'filtering out nodes of degree', limit, 'or higher'

	nodes = nx.nodes(new_graph_obj)
	degrees_dict = nx.degree(new_graph_obj,nodes)
	count = 0
	for node, degree in degrees_dict.iteritems():
		if degree > limit:
			new_graph_obj.remove_node(node)
			count += 1
	print 'removed ', count, 'nodes from the graph'
	print 'writing new graph to ', destination_path
	cPickle.dump(new_graph_obj, open(destination_path,'wb'))
	return new_graph_obj




def filter_voicemail_nodes_graph(csv_file, old_graph, new_graph, user_hash_map, limit=100):
	print 'loading graph from memory:', old_graph
	old_map = cPickle.load(open(old_graph))
	print 'graph loaded.'
	csvData = raw.RawCDRCSV(csv_file)
	new_graph_obj = friend_graph = nx.Graph()
	caller_index = 0
	comm_type_index = 10
	receiver_index = 16
	# WHEN A-->B-->C exists where B is unneccesary, replace
	# with A-->C
	added_edges = 0
	skipped_edges = 0
	print 'csv file rows...'
	for row in csvData.rows_generator():
		caller = row[caller_index]
		receiver = row[receiver_index]
		comm_type = row[comm_type_index]
		# FIRST TWO CASES, REPLACING A--->B with A-->C
		if (comm_type == 'MOC') and (receiver in user_hash_map):
			new_neighbor = user_hash_map[receiver]
			if get_graph_distance(caller, new_neighbor, old_map) == 2:
				added_edges += 1
				new_graph_obj.add_edge(caller,new_neighbor)	
			else:
				new_graph_obj.add_edge(caller,receiver)
		elif (comm_type == 'MTC') and (caller in user_hash_map):
			new_neighbor = user_hash_map[caller]
			if get_graph_distance(receiver, new_neighbor, old_map) == 2:
				added_edges += 1
				new_graph_obj.add_edge(new_neighbor, receiver)
			else:
				new_graph_obj.add_edge(caller,receiver)
		# NEXT TWO CASES checking to not add B-->C
		elif (comm_type == 'MOC') and (caller in user_hash_map):
			if user_hash_map[caller] != receiver:
				new_graph_obj.add_edge(caller,receiver)
			else:
				skipped_edges += 1
		elif (comm_type == 'MOC') and (receiver in user_hash_map):
			if user_hash_map[receiver] != caller:
				new_graph_obj.add_edge(caller,receiver)
			else:
				skipped_edges += 1
		else:
			new_graph_obj.add_edge(caller,receiver)

	print 'skipped ', skipped_edges, 'edges'
	print 'added ', added_edges, 'new edges'
	print 'filtering out nodes of degree', limit, 'or higher'

	nodes = nx.nodes(new_graph_obj)
	degrees_dict = nx.degree(new_graph_obj,nodes)
	count = 0
	for node, degree in degrees_dict.iteritems():
		if degree > limit:
			new_graph_obj.remove_node(node)
			count += 1
	print 'removed ', count, 'nodes from the graph'
	print 'writing new graph to ', destination_path
	cPickle.dump(new_graph_obj, open(destination_path,'wb'))
	return new_graph_obj

def get_graph_distance(user1, user2, friend_graph):
	# -1 means they are infinitely far apart (not connected)
	# -2 means that one of the users was pruned from the graph
	if (user1 in friend_graph) and (user2 in friend_graph):
		try:
			return nx.shortest_path_length(friend_graph, source=user1, target=user2)
		except Exception as e:
			return -1
	return -2

def main():
	# new_obj_clean = '../niquo_data/filtered_data/network_object_cleaned.p'
	# encs_csv = '../niquo_data/CURRENT_DATA/encs_data/2016.07.15_2016.07.21_encounter_n_2.csv'
	# dest = '../niquo_data/CURRENT_DATA/friend_dis_n2.csv'
	# get_distribution_encounters(encs_csv,new_obj_clean,dest)

	# old_graph = '../niquo_data/filtered_data/network_object_100.p'
	new_graph = '../niquo_data/filtered_data/network_object_100_removed_voicemail_UPDATED.p'
	filtered_data = '../niquo_data/filtered_data/06_2017_no_data.csv'
	INTERMEDIATE_DOUBLES = '../niquo_data/filtered_data/INTERMEDIATE_DOUBLES.csv'
	user_hash_dict = fv.create_voicemail_dict(fv.outgoing_only)
	make_new_csv_without_B(filtered_data, user_hash_dict, INTERMEDIATE_DOUBLES)
	make_new_friendship_graph(INTERMEDIATE_DOUBLES,new_graph, float('inf'))





if __name__ == '__main__':
    main()