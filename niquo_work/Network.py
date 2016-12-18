import networkx as nx
import RawCRDData as raw
import file_constants as constants
import filter_voicemail as fv
import cPickle
import csv

def create_graph(csv_file,store_path):
	csvData = raw.RawCDRCSV(csv_file)
	friend_graph = nx.Graph()
	caller_index = 0
	receiver_index = 16

	for row in csvData.rows_generator():
		caller = row[caller_index]
		receiver = row[receiver_index]
		friend_graph.add_edge(caller,receiver)
	cPickle.dump(friend_graph, open(store_path,'wb'))
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

def filter_voicemail_nodes_graph(csv_file, old_graph, new_graph, user_hash_map, limit=100):
	old_map = cPickle.load(open(old_graph))
	csvData = raw.RawCDRCSV(csv_file)
	new_graph_obj = friend_graph = nx.Graph()
	caller_index = 0
	comm_type_index = 10
	receiver_index = 16
	# WHEN A-->B-->C exists where B is unneccesary, replace
	# with A-->C
	added_edges = 0
	skipped_edges = 0

	for row in csvData.rows_generator():
		caller = row[caller_index]
		receiver = row[receiver_index]
		comm_type = row[comm_type_index]
		# FIRST TWO CASES, REPLACING A--->B with A-->C
		if (comm_type == 'MOC') and (receiver in user_hash_map):
			new_neighbor = user_hash_map[receiver]
			if get_graph_distance(caller, new_neighbor) == 2:
				add_edge += 1
				new_graph_obj.add_edge(caller,new_neighbor)	
			else:
				new_graph_obj.add_edge(caller,receiver)
		elif (comm_type == 'MTC') and (caller in user_hash_map):
			new_neighbor = user_hash_map[caller]
			if get_graph_distance(receiver, new_neighbor) == 2:
				add_edge += 1
				new_graph_obj.add_edge(new_neighbor, receiver)
			else:
				new_graph_obj.add_edge(caller,receiver)
		# NEXT TWO CASES checking to not add B--> C
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
	print 'writing new graph to ', new_graph
	cPickle.dump(new_graph_obj, open(new_graph,'wb'))
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

	old_graph = '../niquo_data/filtered_data/network_object_100.p'
	new_graph = '../niquo_data/filtered_data/network_object_100_removed_voicemail.p'
	filtered_data = '../niquo_data/filtered_data/06_2017_no_data.csv'
	user_hash_dict = fv.create_voicemail_dict(fv.outgoing_only)
	filter_voicemail_nodes_graph(filtered_data, old_graph, new_graph, user_hash_dict, limit=100):




if __name__ == '__main__':
    main()